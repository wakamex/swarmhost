#!/usr/bin/env python3
"""Swarm orchestrator: N concurrent Claude workers coordinated through beads.

A planner agent refills the beads task queue when it runs low, giving each
planning round full context of what's been done and what's in flight.  Workers
each get an isolated git worktree and a single task to implement, test, commit,
push, and PR.

Usage:
    python swarm.py "add comprehensive test coverage" --concurrency 5
    python swarm.py "migrate from moment to date-fns" -c 5 -n 30
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import shutil
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

NY_TZ = ZoneInfo("America/New_York")


# ── ANSI ─────────────────────────────────────────────────────────────────────

class C:
    """ANSI color/style codes."""
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    RED     = "\033[31m"
    CYAN    = "\033[36m"
    MAGENTA = "\033[35m"
    BLUE    = "\033[34m"
    WHITE   = "\033[37m"
    BG_GREEN  = "\033[42m"
    BG_BLUE   = "\033[44m"
    BG_DIM    = "\033[100m"


SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
PLANNER_FRAMES = ["🧠", "💭", "🔍", "📋"]


# ── Data types ───────────────────────────────────────────────────────────────

@dataclass
class Task:
    id: str
    title: str
    description: str
    status: str  # open, in_progress, closed
    priority: str = "2"


@dataclass
class CompletedTask:
    id: str
    title: str
    slot: int
    duration: float  # seconds
    result: str  # success, failed, timeout
    pr_url: str = ""


@dataclass
class WorkerState:
    slot: int
    task: Task | None = None
    process: asyncio.subprocess.Process | None = None
    worktree: Path | None = None
    branch: str | None = None
    start_time: float = 0.0
    result: str = "idle"  # idle, running, success, failed, timeout
    pr_url: str = ""


@dataclass
class SwarmState:
    goal: str
    epic_id: str = ""
    concurrency: int = 5
    target: int = 0
    completed: int = 0
    failed: int = 0
    repo: Path = field(default_factory=lambda: Path.cwd())
    workers: list[WorkerState] = field(default_factory=list)
    planner_running: bool = False
    planner_start_time: float = 0.0
    planner_process: asyncio.subprocess.Process | None = None
    planner_runs: int = 0
    start_time: float = field(default_factory=time.monotonic)
    shutdown: bool = False      # first Ctrl-C: drain
    force_quit: bool = False    # second Ctrl-C: kill now
    history: list[CompletedTask] = field(default_factory=list)
    events: deque = field(default_factory=lambda: deque(maxlen=8))
    tick: int = 0  # animation frame counter
    last_children: list[dict] = field(default_factory=list)


# ── bd helpers ───────────────────────────────────────────────────────────────

async def bd(*args: str, cwd: Path | None = None, check: bool = True) -> str:
    """Run a bd command and return stdout."""
    proc = await asyncio.create_subprocess_exec(
        "bd", *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await proc.communicate()
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"bd {' '.join(args)} failed (rc={proc.returncode}): {stderr.decode().strip()}"
        )
    return stdout.decode().strip()


async def bd_json(*args: str, cwd: Path | None = None) -> list | dict:
    """Run a bd command with --json and parse the output."""
    raw = await bd(*args, "--json", cwd=cwd)
    if not raw:
        return []
    return json.loads(raw)


async def get_ready_tasks(epic_id: str, limit: int = 50, cwd: Path | None = None) -> list[Task]:
    """Get tasks that are ready to be worked on."""
    data = await bd_json("ready", "--parent", epic_id, "--limit", str(limit), cwd=cwd)
    if isinstance(data, dict):
        data = data.get("issues", data.get("items", [data]))
    if not isinstance(data, list):
        data = [data]
    tasks = []
    for item in data:
        if isinstance(item, dict):
            tasks.append(Task(
                id=item.get("id", ""),
                title=item.get("title", ""),
                description=item.get("description", ""),
                status=item.get("status", "open"),
                priority=str(item.get("priority", "2")),
            ))
    return tasks


async def get_all_children(epic_id: str, cwd: Path | None = None) -> list[dict]:
    """Get all children of the epic (all statuses)."""
    data = await bd_json("list", "--parent", epic_id, "--all", "--limit", "0", cwd=cwd)
    if isinstance(data, dict):
        data = data.get("issues", data.get("items", []))
    if not isinstance(data, list):
        data = []
    return data


# ── Display ──────────────────────────────────────────────────────────────────

def fmt_elapsed(seconds: float) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def fmt_duration_short(seconds: float) -> str:
    """Compact duration for history lines."""
    m, s = divmod(int(seconds), 60)
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def progress_bar(done: int, total: int, width: int = 30) -> str:
    if total == 0:
        return f"{C.DIM}{'░' * width}{C.RESET}"
    filled = int(width * done / total)
    bar = f"{C.GREEN}{'█' * filled}{C.RESET}{C.DIM}{'░' * (width - filled)}{C.RESET}"
    pct = done * 100 // total
    return f"{bar} {pct}%"


def truncate(s: str, maxlen: int) -> str:
    return s if len(s) <= maxlen else s[: maxlen - 1] + "…"


def render_status(state: SwarmState) -> str:
    now = time.monotonic()
    elapsed = fmt_elapsed(now - state.start_time)
    tick = state.tick
    children = state.last_children

    done_n = sum(1 for c in children if c.get("status") == "closed") if children else state.completed
    in_prog_n = sum(1 for c in children if c.get("status") == "in_progress") if children else 0
    queued_n = sum(1 for c in children if c.get("status") == "open") if children else 0
    total_n = done_n + in_prog_n + queued_n

    # Stats
    success_times = [h.duration for h in state.history if h.result == "success"]
    avg_time = sum(success_times) / len(success_times) if success_times else 0
    elapsed_total = now - state.start_time
    throughput = (state.completed / elapsed_total * 3600) if elapsed_total > 60 else 0

    lines: list[str] = []
    lines.append("")
    if state.shutdown:
        lines.append(f"  {C.BOLD}{C.YELLOW}SHUTTING DOWN{C.RESET}"
                     f"  {C.DIM}waiting for workers to finish...{C.RESET}")
    lines.append(f"  {C.BOLD}{C.CYAN}swarm{C.RESET}  {C.DIM}\"{truncate(state.goal, 60)}\"{C.RESET}")
    lines.append(f"  {C.DIM}epic{C.RESET}   {state.epic_id}  {C.DIM}|{C.RESET}  {elapsed}")
    lines.append("")
    lines.append(f"  {progress_bar(done_n, total_n)}"
                 f"  {C.GREEN}{done_n}{C.RESET} done"
                 f"  {C.YELLOW}{in_prog_n}{C.RESET} running"
                 f"  {C.DIM}{queued_n} queued{C.RESET}")
    if success_times:
        lines.append(f"  {C.DIM}avg {fmt_duration_short(avg_time)}/task"
                     f"  {throughput:.1f} tasks/hr"
                     f"  {len(success_times)} ok  {state.failed} fail{C.RESET}")
        remaining = in_prog_n + queued_n
        if remaining > 0 and throughput > 0:
            eta_seconds = remaining / throughput * 3600
            eta_time = datetime.datetime.now(NY_TZ) + datetime.timedelta(seconds=eta_seconds)
            lines.append(f"  {C.DIM}eta {fmt_elapsed(eta_seconds)}"
                         f"  ~{eta_time.strftime('%-I:%M %p')} ET{C.RESET}")
    lines.append("")

    # Workers
    lines.append(f"  {C.BOLD}workers{C.RESET}")
    for w in state.workers:
        if w.result == "running" and w.task:
            wt = fmt_elapsed(now - w.start_time)
            spin = SPINNER[tick % len(SPINNER)]
            title = truncate(w.task.title, 45)
            lines.append(
                f"  {C.YELLOW}{spin}{C.RESET} {C.DIM}worker-{w.slot}{C.RESET}"
                f"  {w.task.id}  {title}"
                f"  {C.DIM}{wt}{C.RESET}"
            )
        elif w.result == "success" and w.task:
            pr = f"  {C.CYAN}{w.pr_url}{C.RESET}" if w.pr_url else ""
            title = truncate(w.task.title, 45)
            lines.append(
                f"  {C.GREEN}✓{C.RESET} {C.DIM}worker-{w.slot}{C.RESET}"
                f"  {w.task.id}  {title}{pr}"
            )
        elif w.result in ("failed", "timeout") and w.task:
            label = "TIMEOUT" if w.result == "timeout" else "FAILED"
            title = truncate(w.task.title, 45)
            lines.append(
                f"  {C.RED}✗{C.RESET} {C.DIM}worker-{w.slot}{C.RESET}"
                f"  {w.task.id}  {title}"
                f"  {C.RED}{label}{C.RESET}"
            )
        else:
            lines.append(f"  {C.DIM}· worker-{w.slot}  idle{C.RESET}")

    # Planner
    lines.append("")
    if state.planner_running:
        pframe = PLANNER_FRAMES[tick % len(PLANNER_FRAMES)]
        pt = fmt_elapsed(now - state.planner_start_time)
        lines.append(f"  {pframe} {C.MAGENTA}planner{C.RESET}  thinking...  {C.DIM}{pt}{C.RESET}")
    else:
        lines.append(f"  {C.DIM}  planner  idle  ({state.planner_runs} runs){C.RESET}")

    # Event log
    if state.events:
        lines.append("")
        lines.append(f"  {C.BOLD}recent{C.RESET}")
        for ev in state.events:
            lines.append(f"  {C.DIM}{ev}{C.RESET}")

    lines.append("")
    return "\n".join(lines)


def display(state: SwarmState) -> None:
    """Print status to stderr."""
    sys.stderr.write("\033[2J\033[H")
    sys.stderr.write(render_status(state))
    sys.stderr.flush()


def event(state: SwarmState, msg: str) -> None:
    """Record a timestamped event for the display and log file."""
    ts = time.strftime("%H:%M:%S")
    state.events.append(f"[{ts}] {msg}")
    log(msg)


# ── Planner ──────────────────────────────────────────────────────────────────

def build_planner_prompt(
    goal: str,
    epic_id: str,
) -> str:
    return f"""\
You are a planner for a swarm of AI coding agents. Your job is to create the
next batch of tasks for the swarm to work on.

## High-level goal

{goal}

## Current state

Run this command to see all existing tasks (done, in-progress, and open):

  bd list --parent {epic_id} --all --json

Review the output before creating new tasks.

## Instructions

1. Review what's been completed and what's in flight.
2. Explore the codebase to find the next best improvements toward the goal.
3. Create 8-15 tasks using `bd create`, each scoped to a single PR-worthy chunk.
4. Each task should be actionable by an independent worker with no shared state.
5. Include file paths and hints in each task description so workers know where to start.
6. Use `--parent {epic_id}` on every `bd create` command.
7. If some tasks should precede others, use `bd dep add <blocked> <blocker>`.
8. Do NOT duplicate work that's already done or queued.
9. If the goal is fully achieved (all reasonable work is done), create zero tasks
   and instead output the line: SWARM_GOAL_COMPLETE

Example create command:
  bd create "Add unit tests for auth/login.py" \\
    --parent {epic_id} \\
    -d "Add comprehensive unit tests for the login module at auth/login.py. \\
        Cover: successful login, invalid password, locked account, rate limiting. \\
        Use pytest. Target >90% coverage for this file."

Create the tasks now. Output each bd command, then run it.
"""


async def run_planner(
    state: SwarmState,
    *,
    model: str,
    budget: float,
) -> bool:
    """Spawn a planner Claude session. Returns True if goal is complete."""
    prompt = build_planner_prompt(state.goal, state.epic_id)

    cmd = [
        "claude", "-p",
        "--model", model,
        "--allowedTools", "Bash Read Glob Grep",
        "--dangerously-skip-permissions",
    ]
    if budget > 0:
        cmd.extend(["--max-budget-usd", str(budget)])

    state.planner_running = True
    state.planner_start_time = time.monotonic()
    state.planner_runs += 1
    event(state, f"planner run #{state.planner_runs} started")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=state.repo,
        )
        state.planner_process = proc
        stdout, stderr = await proc.communicate(input=prompt.encode())
        output = stdout.decode()
        dur = time.monotonic() - state.planner_start_time

        if "SWARM_GOAL_COMPLETE" in output:
            event(state, f"planner says GOAL COMPLETE ({fmt_elapsed(dur)})")
            return True
        if proc.returncode != 0:
            event(state, f"planner exited with code {proc.returncode} ({fmt_elapsed(dur)})")
            if stderr:
                log(f"planner stderr: {stderr.decode()[:500]}")
        else:
            event(state, f"planner finished ({fmt_elapsed(dur)})")
        return False
    finally:
        state.planner_running = False
        state.planner_process = None


# ── Merger ───────────────────────────────────────────────────────────────

def build_merger_prompt(goal: str, epic_id: str, pr_urls: list[str]) -> str:
    pr_list = "\n".join(f"  - {url}" for url in pr_urls)
    return f"""\
You are a merger agent for a swarm of AI coding agents. Workers have completed
tasks and created PRs. Your job is to review and merge them so that dependent
tasks can proceed.

## High-level goal

{goal}

## PRs to review and merge

{pr_list}

## Instructions

For each PR above:

1. Review the diff:
   gh pr diff <number>

2. Check that it looks reasonable:
   - Code follows existing patterns and conventions
   - No obvious bugs, security issues, or broken imports
   - Tests are included and pass
   - The change matches what the PR title/body describes
   - No unrelated changes snuck in

3. Run the test suite to verify nothing is broken:
   npm run test:unit  (or whatever the project uses — check package.json / Makefile)

4. If the PR looks good, merge it:
   gh pr merge <number> --squash --delete-branch

5. If the PR has issues, close it and reopen the beads task with notes:
   gh pr close <number> --comment "Issues found: <description>"
   Then find the beads task ID from the PR body (it says "Implements <id>")
   and reopen it with notes so the next worker knows what to fix:
   bd reopen <id>
   bd update <id> --append-notes "Merger feedback: <what needs fixing>"

6. After merging all good PRs, update main:
   git pull origin main

Process them in dependency order — if PR A's code is needed by PR B,
merge A first.

Review the current task state to understand dependencies:
  bd list --parent {epic_id} --all --json
"""


async def get_blocked_tasks(epic_id: str, cwd: Path | None = None) -> list[dict]:
    """Get tasks that are blocked (have unresolved dependencies)."""
    data = await bd_json("blocked", "--json", cwd=cwd)
    if isinstance(data, dict):
        data = data.get("issues", data.get("items", []))
    if not isinstance(data, list):
        data = []
    # Filter to our epic's children
    children = await get_all_children(epic_id, cwd=cwd)
    child_ids = {c.get("id") for c in children}
    return [t for t in data if t.get("id") in child_ids]


async def get_mergeable_prs(epic_id: str, cwd: Path | None = None) -> list[str]:
    """Find PR URLs from closed beads whose dependents are still blocked.

    A bead is "mergeable" if:
    - It's closed (worker finished, PR created)
    - Its close_reason contains a PR URL
    - It has dependents that are still open/blocked
    """
    children = await get_all_children(epic_id, cwd=cwd)

    # Build maps
    closed_with_pr: dict[str, str] = {}  # bead_id -> pr_url
    open_ids = set()
    for c in children:
        cid = c.get("id", "")
        status = c.get("status", "")
        reason = c.get("close_reason", "") or ""
        if status == "closed" and "github.com" in reason and "/pull/" in reason:
            # Extract URL from reason
            for word in reason.split():
                if "github.com" in word and "/pull/" in word:
                    closed_with_pr[cid] = word.strip()
                    break
        elif status in ("open", "in_progress", "blocked"):
            open_ids.add(cid)

    if not closed_with_pr or not open_ids:
        return []

    # Check which closed beads have open dependents
    # Use bd dep list to find dependencies
    mergeable = []
    for bead_id, pr_url in closed_with_pr.items():
        try:
            dep_data = await bd_json("dep", "list", bead_id, cwd=cwd)
            if isinstance(dep_data, dict):
                # Check if this bead blocks any open tasks
                blocks = dep_data.get("blocks", [])
                if isinstance(blocks, list):
                    for blocked in blocks:
                        blocked_id = blocked.get("id", blocked) if isinstance(blocked, dict) else str(blocked)
                        if blocked_id in open_ids:
                            mergeable.append(pr_url)
                            break
        except Exception:
            # If we can't check deps, include it anyway — merger will sort it out
            mergeable.append(pr_url)

    return mergeable


async def run_merger(
    state: SwarmState,
    pr_urls: list[str],
    *,
    model: str,
    budget: float,
) -> None:
    """Spawn a merger Claude session to review and merge PRs."""
    prompt = build_merger_prompt(state.goal, state.epic_id, pr_urls)

    cmd = [
        "claude", "-p",
        "--model", model,
        "--allowedTools", "Bash Read Glob Grep",
        "--dangerously-skip-permissions",
    ]
    if budget > 0:
        cmd.extend(["--max-budget-usd", str(budget)])

    state.merger_running = True
    state.merger_start_time = time.monotonic()
    state.merger_runs += 1
    event(state, f"merger reviewing {len(pr_urls)} PRs")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=state.repo,
        )
        state.merger_process = proc
        stdout, stderr = await proc.communicate(input=prompt.encode())
        dur = time.monotonic() - state.merger_start_time

        if proc.returncode != 0:
            event(state, f"merger exited with code {proc.returncode} ({fmt_elapsed(dur)})")
            if stderr:
                log(f"merger stderr: {stderr.decode()[:500]}")
        else:
            event(state, f"merger finished ({fmt_elapsed(dur)})")

        # Pull main to pick up merged changes
        pull = await asyncio.create_subprocess_exec(
            "git", "pull", "origin", "main",
            cwd=state.repo,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await pull.communicate()

    finally:
        state.merger_running = False
        state.merger_process = None


# ── Worker ───────────────────────────────────────────────────────────────────

def build_worker_prompt(task: Task, slot: int, *, git_name: str, git_email: str) -> str:
    return f"""\
You are worker-{slot} in a swarm of AI coding agents. You have been assigned
a single task to implement.

## Your task

ID: {task.id}
Title: {task.title}

Description:
{task.description}

## Instructions

1. First, claim the task:
   bd update {task.id} --claim --actor worker-{slot}

2. Implement ONLY the change described above — nothing more.
   - Read relevant files first, understand the code.
   - Make the minimal, focused changes needed.
   - Follow existing code style and conventions.
   - Do NOT work on anything outside your assigned task.
   - Other modules are being handled by other workers.

3. Run the project's test suite. Fix any failures your changes introduced.

4. Commit with a clear message (use this exact git identity):
   git add -A
   git -c user.name="{git_name}-{slot}" -c user.email="{git_email}" commit -m "<descriptive message>"

5. Push your branch:
   git push -u origin HEAD

6. Create a PR:
   gh pr create --title "{task.title}" --body "Implements {task.id}: {task.title}"

7. Close the beads task with the PR URL:
   bd close {task.id} --reason "PR: <paste the PR URL here>"

If anything fails and you cannot complete the task, release it:
   bd update {task.id} --status open --assignee ""
   Then explain what went wrong.
"""


async def run_worker(
    state: SwarmState,
    worker: WorkerState,
    task: Task,
    *,
    model: str,
    budget: float,
    timeout: int,
    git_name: str,
    git_email: str,
) -> None:
    """Run a single worker in its worktree."""
    worker.task = task
    worker.result = "running"
    worker.pr_url = ""
    worker.start_time = time.monotonic()

    ts = int(time.time())
    branch = f"swarm/worker-{worker.slot}-{ts}"
    worktree_dir = state.repo / ".swarm" / "worktrees" / f"worker-{worker.slot}"
    worker.branch = branch
    worker.worktree = worktree_dir

    event(state, f"worker-{worker.slot} starting {task.id} \"{truncate(task.title, 40)}\"")

    # Clean up stale worktree at this path if it exists
    if worktree_dir.exists():
        try:
            await asyncio.create_subprocess_exec(
                "git", "worktree", "remove", "--force", str(worktree_dir),
                cwd=state.repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except Exception:
            pass

    # Create worktree
    proc = await asyncio.create_subprocess_exec(
        "git", "worktree", "add", "-b", branch, str(worktree_dir),
        cwd=state.repo,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        dur = time.monotonic() - worker.start_time
        event(state, f"worker-{worker.slot} worktree failed: {stderr.decode().strip()[:80]}")
        worker.result = "failed"
        state.failed += 1
        state.history.append(CompletedTask(
            task.id, task.title, worker.slot, dur, "failed"))
        await bd("update", task.id, "--status", "open", "--assignee", "",
                 cwd=state.repo, check=False)
        return

    # Set git identity in the worktree so all commits use it
    await asyncio.create_subprocess_exec(
        "git", "config", "user.name", f"{git_name}-{worker.slot}",
        cwd=worktree_dir, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await asyncio.create_subprocess_exec(
        "git", "config", "user.email", git_email,
        cwd=worktree_dir, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)

    prompt = build_worker_prompt(task, worker.slot, git_name=git_name, git_email=git_email)
    cmd = [
        "claude", "-p",
        "--model", model,
        "--allowedTools", "Bash Read Write Edit Glob Grep",
        "--dangerously-skip-permissions",
    ]
    if budget > 0:
        cmd.extend(["--max-budget-usd", str(budget)])

    try:
        worker_proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=worktree_dir,
        )
        worker.process = worker_proc

        prompt_bytes = prompt.encode()
        if timeout > 0:
            try:
                stdout_data, stderr_out = await asyncio.wait_for(
                    worker_proc.communicate(input=prompt_bytes),
                    timeout=timeout * 60,
                )
            except asyncio.TimeoutError:
                worker_proc.kill()
                await worker_proc.wait()
                dur = time.monotonic() - worker.start_time
                event(state, f"worker-{worker.slot} TIMEOUT after {fmt_elapsed(dur)} on {task.id}")
                worker.result = "timeout"
                state.failed += 1
                state.history.append(CompletedTask(
                    task.id, task.title, worker.slot, dur, "timeout"))
                await bd("update", task.id, "--status", "open", "--assignee", "",
                         cwd=state.repo, check=False)
                return
        else:
            stdout_data, stderr_out = await worker_proc.communicate(input=prompt_bytes)

        dur = time.monotonic() - worker.start_time
        output = stdout_data.decode()

        # Check if the task was closed in beads
        try:
            task_data = await bd_json("show", task.id, cwd=state.repo)
            task_status = task_data.get("status", "") if isinstance(task_data, dict) else ""
        except Exception:
            task_status = ""

        # Extract PR URL from output
        pr_url = ""
        for line in output.splitlines():
            if "github.com" in line and "/pull/" in line:
                # Pull out just the URL
                for word in line.split():
                    if "github.com" in word and "/pull/" in word:
                        pr_url = word.strip()
                        break
                if pr_url:
                    break

        if task_status == "closed":
            worker.result = "success"
            worker.pr_url = pr_url
            state.completed += 1
            state.history.append(CompletedTask(
                task.id, task.title, worker.slot, dur, "success", pr_url))
            pr_note = f"  PR: {pr_url}" if pr_url else ""
            event(state, f"worker-{worker.slot} ✓ {task.id} ({fmt_duration_short(dur)}){pr_note}")
        else:
            worker.result = "failed"
            state.failed += 1
            state.history.append(CompletedTask(
                task.id, task.title, worker.slot, dur, "failed"))
            rc = worker_proc.returncode
            event(state, f"worker-{worker.slot} ✗ {task.id} FAILED ({fmt_duration_short(dur)}, exit {rc}, bead {task_status or 'unknown'})")
            await bd("update", task.id, "--status", "open", "--assignee", "",
                     cwd=state.repo, check=False)

    except Exception as exc:
        dur = time.monotonic() - worker.start_time
        worker.result = "failed"
        state.failed += 1
        state.history.append(CompletedTask(
            task.id, task.title, worker.slot, dur, "failed"))
        event(state, f"worker-{worker.slot} exception on {task.id}: {exc}")
        await bd("update", task.id, "--status", "open", "--assignee", "",
                 cwd=state.repo, check=False)
    finally:
        worker.process = None


async def cleanup_worktree(state: SwarmState, worker: WorkerState) -> None:
    """Remove a worker's worktree. Keep on failure for inspection."""
    if not worker.worktree:
        return
    if worker.result == "success":
        try:
            await asyncio.create_subprocess_exec(
                "git", "worktree", "remove", "--force", str(worker.worktree),
                cwd=state.repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except Exception:
            pass
    else:
        if worker.worktree and worker.worktree.exists():
            event(state, f"worker-{worker.slot} worktree kept: {worker.worktree}")


# ── Logging ──────────────────────────────────────────────────────────────────

_log_file: Path | None = None


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    if _log_file:
        with open(_log_file, "a") as f:
            f.write(line + "\n")


# ── Bootstrap ────────────────────────────────────────────────────────────────

async def verify_tools() -> None:
    """Check that bd and claude are in PATH."""
    for tool in ("bd", "claude", "git", "gh"):
        if not shutil.which(tool):
            print(f"Error: '{tool}' not found in PATH", file=sys.stderr)
            sys.exit(1)


async def ensure_beads(repo: Path) -> None:
    """Check that beads is working, initialize if needed."""
    # Try a quick read command to see if bd is already set up
    result = await bd("status", cwd=repo, check=False)
    if "no beads database" in result.lower() or "not found" in result.lower():
        log("Initializing beads database...")
        await bd("init", cwd=repo)


async def create_epic(goal: str, repo: Path) -> str:
    """Create the swarm epic and return its ID."""
    # bd title limit is 500 chars — use a short title, put the full goal in description
    title = goal[:200].rsplit(" ", 1)[0] if len(goal) > 200 else goal
    raw = await bd("create", title, "-t", "epic", "-d", goal, "--json", cwd=repo)
    data = json.loads(raw)
    epic_id = data.get("id", "")
    if not epic_id:
        raise RuntimeError(f"Failed to get epic ID from: {raw}")
    return epic_id


# ── Display refresh loop ────────────────────────────────────────────────────

async def display_loop(state: SwarmState) -> None:
    """Background task: refresh the display every 500ms for live animation."""
    while not state.shutdown:
        state.tick += 1
        # Periodically refresh children from beads (every 10 ticks = 5s)
        if state.tick % 10 == 0 and state.epic_id:
            try:
                state.last_children = await get_all_children(
                    state.epic_id, cwd=state.repo)
            except Exception:
                pass
        display(state)
        await asyncio.sleep(0.5)


# ── Main loop ────────────────────────────────────────────────────────────────

async def main_loop(state: SwarmState, args: argparse.Namespace) -> None:
    refill_threshold = args.refill if args.refill > 0 else state.concurrency
    planner_task: asyncio.Task | None = None
    worker_tasks: dict[int, asyncio.Task] = {}  # slot -> asyncio.Task

    # Start the display refresh loop
    display_task = asyncio.create_task(display_loop(state))

    # Initial planner run — must finish before workers start
    event(state, "running initial planner to create first batch...")
    goal_complete = await run_planner(
        state,
        model=args.planner_model,
        budget=args.budget * 3,
    )
    if goal_complete:
        event(state, "planner says goal is already complete")
        state.shutdown = True
        display_task.cancel()
        return

    ready = await get_ready_tasks(state.epic_id, cwd=state.repo)
    if not ready:
        event(state, "planner created no tasks — exiting")
        state.shutdown = True
        display_task.cancel()
        return

    event(state, f"initial batch ready: {len(ready)} tasks")
    state.last_children = await get_all_children(state.epic_id, cwd=state.repo)

    try:
        while not state.shutdown:
            # Check target
            if state.target > 0 and state.completed >= state.target:
                event(state, f"reached target of {state.target} completed tasks")
                break

            # Get current state
            try:
                ready = await get_ready_tasks(state.epic_id, cwd=state.repo)
            except Exception as exc:
                log(f"Error getting ready tasks: {exc}")
                ready = []

            ready_count = len(ready)

            # Trigger planner if queue is low
            if (ready_count < refill_threshold
                    and not state.planner_running
                    and (planner_task is None or planner_task.done())):
                event(state, f"queue low ({ready_count}<{refill_threshold}), spawning planner")

                async def _run_planner():
                    return await run_planner(
                        state,
                        model=args.planner_model,
                        budget=args.budget * 3,
                    )

                planner_task = asyncio.create_task(_run_planner())

            # Check if we're stuck
            running_workers = sum(1 for t in worker_tasks.values() if not t.done())
            if ready_count == 0 and running_workers == 0:
                if state.planner_running and planner_task and not planner_task.done():
                    event(state, "no tasks, no workers — waiting for planner")
                    goal_complete = await planner_task
                    planner_task = None
                    if goal_complete:
                        event(state, "planner says goal is complete")
                        break
                    ready = await get_ready_tasks(state.epic_id, cwd=state.repo)
                    if not ready:
                        event(state, "planner finished but no new tasks — done")
                        break
                    continue
                else:
                    event(state, "no tasks, no workers, planner idle — done")
                    break

            # Assign tasks to idle worker slots
            ready_idx = 0
            for w in state.workers:
                if state.target > 0 and state.completed >= state.target:
                    break
                slot = w.slot
                if slot in worker_tasks and not worker_tasks[slot].done():
                    continue
                if ready_idx >= len(ready):
                    break

                task = ready[ready_idx]
                ready_idx += 1

                if slot in worker_tasks and worker_tasks[slot].done():
                    await cleanup_worktree(state, w)

                worker_tasks[slot] = asyncio.create_task(
                    run_worker(
                        state, w, task,
                        model=args.model,
                        budget=args.budget,
                        timeout=args.timeout,
                        git_name=args.git_name,
                        git_email=args.git_email,
                    )
                )

            # Wait for any task to finish
            pending: set[asyncio.Task] = set()
            for t in worker_tasks.values():
                if not t.done():
                    pending.add(t)
            if planner_task and not planner_task.done():
                pending.add(planner_task)

            if not pending:
                await asyncio.sleep(1)
                continue

            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for t in done:
                if planner_task and t is planner_task:
                    try:
                        goal_complete = t.result()
                        if goal_complete:
                            event(state, "planner says goal is complete")
                            state.shutdown = True
                    except Exception as exc:
                        event(state, f"planner failed: {exc}")
                        # Cooldown: don't retry immediately on failure
                        event(state, "planner cooldown 30s before retry")
                        await asyncio.sleep(30)
                    planner_task = None
                else:
                    for slot, wt in worker_tasks.items():
                        if wt is t:
                            try:
                                t.result()
                            except Exception as exc:
                                event(state, f"worker-{slot} raised: {exc}")
                            break

            # Refresh children after events
            try:
                state.last_children = await get_all_children(
                    state.epic_id, cwd=state.repo)
            except Exception:
                pass

    finally:
        # ── Shutdown ─────────────────────────────────────────────────────
        state.shutdown = True

        # Kill planner immediately — no point waiting for new tasks
        if planner_task and not planner_task.done():
            if state.planner_process and state.planner_process.returncode is None:
                state.planner_process.kill()
            planner_task.cancel()
            try:
                await planner_task
            except (asyncio.CancelledError, Exception):
                pass
            event(state, "planner cancelled")

        # Let in-flight workers finish (they're mid-PR, worth waiting)
        running = [t for t in worker_tasks.values() if not t.done()]
        if running and not state.force_quit:
            n = len(running)
            event(state, f"draining {n} in-flight workers (Ctrl-C again to force)...")
            done_set, pending_set = await asyncio.wait(running)
            # If we get here, all finished naturally
        elif running:
            # Force quit — already killed by signal handler, just collect
            event(state, f"force-killing {len(running)} workers")
            for t in running:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

        display_task.cancel()
        try:
            await display_task
        except asyncio.CancelledError:
            pass

        for w in state.workers:
            await cleanup_worktree(state, w)

        # Final display
        try:
            state.last_children = await get_all_children(
                state.epic_id, cwd=state.repo)
        except Exception:
            pass
        display(state)

        # Summary
        success_times = [h.duration for h in state.history if h.result == "success"]
        total_elapsed = time.monotonic() - state.start_time
        summary = (
            f"\nSwarm finished in {fmt_elapsed(total_elapsed)}."
            f"  {state.completed} completed, {state.failed} failed."
        )
        if success_times:
            avg = sum(success_times) / len(success_times)
            fastest = min(success_times)
            slowest = max(success_times)
            summary += (
                f"\n  avg: {fmt_duration_short(avg)}"
                f"  fastest: {fmt_duration_short(fastest)}"
                f"  slowest: {fmt_duration_short(slowest)}"
            )
        sys.stderr.write(summary + "\n")
        sys.stderr.flush()
        log(summary)


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Swarm orchestrator: N concurrent Claude workers coordinated through beads.",
    )
    parser.add_argument("task", nargs="?", default=None,
                        help="The high-level goal (or use --goal-file)")
    parser.add_argument(
        "--goal-file", type=Path, default=None,
        help="Read the goal from a file instead of the command line",
    )
    parser.add_argument(
        "-c", "--concurrency", type=int, default=5,
        help="Max parallel workers (default: 5)",
    )
    parser.add_argument(
        "-n", "--target", type=int, default=0,
        help="Total tasks to complete, 0 = run until planner says done (default: 0)",
    )
    parser.add_argument(
        "--repo", type=Path, default=Path.cwd(),
        help="Git repo path (default: cwd)",
    )
    parser.add_argument(
        "--timeout", type=int, default=0,
        help="Per-worker timeout in minutes, 0 = no timeout (default: 0)",
    )
    parser.add_argument(
        "--refill", type=int, default=0,
        help="Ready-queue threshold that triggers planner (default: equals concurrency)",
    )
    parser.add_argument(
        "--model", default="sonnet",
        help="Claude model for workers (default: sonnet)",
    )
    parser.add_argument(
        "--planner-model", default="opus",
        help="Claude model for planner (default: opus)",
    )
    parser.add_argument(
        "--budget", type=float, default=0,
        help="Max USD per worker session, 0 = unlimited (default: 0)",
    )
    parser.add_argument(
        "--git-name", default="Swarm",
        help="Git author name for worker commits (default: Swarm)",
    )
    parser.add_argument(
        "--git-email", default="swarm@localhost",
        help="Git author email for worker commits (default: swarm@localhost)",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()

    # Resolve goal from positional arg or --goal-file
    if args.goal_file:
        args.task = args.goal_file.read_text().strip()
    if not args.task:
        print("Error: provide a goal as an argument or via --goal-file", file=sys.stderr)
        sys.exit(1)

    repo = args.repo.resolve()

    global _log_file
    log_dir = repo / ".swarm"
    log_dir.mkdir(parents=True, exist_ok=True)
    _log_file = log_dir / "swarm.log"

    await verify_tools()
    await ensure_beads(repo)

    log(f'Starting swarm: "{args.task}"')
    log(f"  concurrency={args.concurrency}  target={args.target or 'unlimited'}"
        f"  timeout={args.timeout or 'none'}  model={args.model}  planner={args.planner_model}")

    epic_id = await create_epic(args.task, repo)
    log(f"Created epic: {epic_id}")

    state = SwarmState(
        goal=args.task,
        epic_id=epic_id,
        concurrency=args.concurrency,
        target=args.target,
        repo=repo,
        workers=[WorkerState(slot=i) for i in range(args.concurrency)],
    )

    def _handle_signal():
        if state.shutdown:
            # Second signal — force quit
            state.force_quit = True
            event(state, "FORCE QUIT — killing workers...")
            for w in state.workers:
                if w.process and w.process.returncode is None:
                    try:
                        w.process.kill()
                    except Exception:
                        pass
            if state.planner_process and state.planner_process.returncode is None:
                try:
                    state.planner_process.kill()
                except Exception:
                    pass
        else:
            state.shutdown = True
            event(state, "Ctrl-C: draining in-flight workers (Ctrl-C again to force)")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    await main_loop(state, args)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
