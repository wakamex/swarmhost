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
import os
import re
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

# Clean env for spawning claude subprocesses (avoid nested session detection)
_CLAUDE_ENV = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}


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
    last_line: str = ""
    conflict_ref: str = ""  # non-empty when resolving a merge conflict


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
    planner_last_line: str = ""


# ── Stream helper ────────────────────────────────────────────────────────────

def _extract_status(data: dict) -> str | None:
    """Extract a human-readable status snippet from a stream-json event."""
    etype = data.get("type", "")

    if etype == "assistant":
        msg = data.get("message", {})
        content = msg.get("content", [])
        for block in content:
            btype = block.get("type", "")
            if btype == "tool_use":
                name = block.get("name", "")
                inp = block.get("input", {})
                if name == "Bash":
                    cmd = inp.get("command", "")
                    return f"$ {cmd[:70]}" if cmd else f"[{name}]"
                elif name in ("Read", "Write", "Edit"):
                    path = inp.get("file_path", "")
                    short = path.split("/")[-1] if "/" in path else path
                    return f"{name}: {short}" if short else f"[{name}]"
                elif name in ("Glob", "Grep"):
                    pat = inp.get("pattern", "")
                    return f"{name}: {pat[:50]}" if pat else f"[{name}]"
                else:
                    return f"[{name}]"
            elif btype == "text":
                text = block.get("text", "").strip()
                if text:
                    # Take last non-empty line
                    last = text.rstrip().rsplit("\n", 1)[-1].strip()
                    return last if last else None
            elif btype == "thinking":
                return "thinking..."
    elif etype == "result":
        return None
    return None


async def stream_output(
    proc: asyncio.subprocess.Process,
    prompt_bytes: bytes,
    on_line: callable,
) -> str:
    """Feed stdin, stream stdout as JSON events, call on_line with status snippets, return final result text."""
    assert proc.stdin is not None
    proc.stdin.write(prompt_bytes)
    await proc.stdin.drain()
    proc.stdin.close()

    result_text = ""
    assert proc.stdout is not None
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Not JSON — show raw text
            if text.strip():
                on_line(text.strip())
            continue

        status = _extract_status(data)
        if status:
            on_line(status)

        # Capture final result text
        if data.get("type") == "result":
            result_text = data.get("result", "")

    await proc.wait()
    return result_text


# ── bd helpers ───────────────────────────────────────────────────────────────

async def bd(*args: str, cwd: Path | None = None, check: bool = True) -> str:
    """Run a bd command and return stdout (or stderr on failure with check=False)."""
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
    # Return stderr when stdout is empty and command failed (for check=False callers)
    out = stdout.decode().strip()
    if not out and proc.returncode != 0:
        out = stderr.decode().strip()
    return out


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
            if w.conflict_ref:
                title = truncate(w.task.title, 45)
                lines.append(
                    f"  {C.BLUE}{spin}{C.RESET} {C.DIM}worker-{w.slot}{C.RESET}"
                    f"  {C.BLUE}merge{C.RESET}  {title}"
                    f"  {C.DIM}{wt}{C.RESET}"
                )
            else:
                title = truncate(w.task.title, 45)
                lines.append(
                    f"  {C.YELLOW}{spin}{C.RESET} {C.DIM}worker-{w.slot}{C.RESET}"
                    f"  {w.task.id}  {title}"
                    f"  {C.DIM}{wt}{C.RESET}"
                )
            if w.last_line:
                lines.append(f"    {C.DIM}{truncate(w.last_line, 72)}{C.RESET}")
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

    # Planner & Merger
    lines.append("")
    if state.planner_running:
        pframe = PLANNER_FRAMES[tick % len(PLANNER_FRAMES)]
        pt = fmt_elapsed(now - state.planner_start_time)
        lines.append(f"  {pframe} {C.MAGENTA}planner{C.RESET}  thinking...  {C.DIM}{pt}{C.RESET}")
        if state.planner_last_line:
            lines.append(f"    {C.DIM}{truncate(state.planner_last_line, 72)}{C.RESET}")
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
        "--output-format", "stream-json", "--verbose",
        "--model", model,
        "--allowedTools", "Bash Read Glob Grep",
        "--dangerously-skip-permissions",
    ]
    if budget > 0:
        cmd.extend(["--max-budget-usd", str(budget)])

    state.planner_running = True
    state.planner_start_time = time.monotonic()
    state.planner_runs += 1
    state.planner_last_line = ""
    event(state, f"planner run #{state.planner_runs} started")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=state.repo,
            env=_CLAUDE_ENV,
            limit=10 * 1024 * 1024,  # 10MB line buffer for stream-json
        )
        state.planner_process = proc

        def _on_line(text: str) -> None:
            state.planner_last_line = text

        output = await stream_output(proc, prompt.encode(), _on_line)
        dur = time.monotonic() - state.planner_start_time

        if "SWARM_GOAL_COMPLETE" in output:
            event(state, f"planner says GOAL COMPLETE ({fmt_elapsed(dur)})")
            return True
        if proc.returncode != 0:
            event(state, f"planner exited with code {proc.returncode} ({fmt_elapsed(dur)})")
        else:
            event(state, f"planner finished ({fmt_elapsed(dur)})")
        return False
    finally:
        state.planner_running = False
        state.planner_process = None


async def get_blocked_tasks(epic_id: str, cwd: Path | None = None) -> list[dict]:
    """Get tasks that are blocked (have unresolved dependencies)."""
    data = await bd_json("blocked", cwd=cwd)
    if isinstance(data, dict):
        data = data.get("issues", data.get("items", []))
    if not isinstance(data, list):
        data = []
    # Filter to our epic's children
    children = await get_all_children(epic_id, cwd=cwd)
    child_ids = {c.get("id") for c in children}
    return [t for t in data if t.get("id") in child_ids]


async def get_mergeable_refs(epic_id: str, *, local: bool = False, cwd: Path | None = None) -> list[str]:
    """Find PR URLs or branch names from closed beads whose dependents are still blocked.

    Returns PR URLs (in PR mode) or branch names (in local mode).

    A bead is "mergeable" if:
    - It's closed (worker finished)
    - Its close_reason contains a PR URL or branch name
    - It has dependents that are still open/blocked
    """
    children = await get_all_children(epic_id, cwd=cwd)

    # Build maps
    closed_with_ref: dict[str, str] = {}  # bead_id -> pr_url or branch
    open_ids = set()
    for c in children:
        cid = c.get("id", "")
        status = c.get("status", "")
        reason = c.get("close_reason", "") or ""
        if status == "closed" and reason:
            if local and "branch:" in reason:
                # Extract branch name
                branch = reason.split("branch:", 1)[1].strip()
                if branch:
                    closed_with_ref[cid] = branch
            elif not local and "github.com" in reason and "/pull/" in reason:
                for word in reason.split():
                    if "github.com" in word and "/pull/" in word:
                        closed_with_ref[cid] = word.strip()
                        break
        elif status in ("open", "in_progress", "blocked"):
            open_ids.add(cid)

    if not closed_with_ref or not open_ids:
        return []

    # Check which closed beads have open dependents
    mergeable = []
    for bead_id, ref in closed_with_ref.items():
        try:
            dep_data = await bd_json("dep", "list", bead_id, cwd=cwd)
            if isinstance(dep_data, dict):
                blocks = dep_data.get("blocks", [])
                if isinstance(blocks, list):
                    for blocked in blocks:
                        blocked_id = blocked.get("id", blocked) if isinstance(blocked, dict) else str(blocked)
                        if blocked_id in open_ids:
                            mergeable.append(ref)
                            break
        except Exception:
            mergeable.append(ref)

    return mergeable


# ── Merge helpers ────────────────────────────────────────────────────────────

async def try_merge_ref(
    ref: str,
    *,
    base_branch: str,
    local: bool,
    cwd: Path,
) -> bool:
    """Try a simple merge of a ref (branch name or PR URL). Returns True on success."""
    if local:
        rebase = await asyncio.create_subprocess_exec(
            "git", "rebase", base_branch, ref,
            cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await rebase.communicate()
        if rebase.returncode != 0:
            await asyncio.create_subprocess_exec(
                "git", "rebase", "--abort", cwd=cwd,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            return False
        await asyncio.create_subprocess_exec(
            "git", "checkout", base_branch, cwd=cwd,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        ff = await asyncio.create_subprocess_exec(
            "git", "merge", "--ff-only", ref,
            cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await ff.communicate()
        return ff.returncode == 0
    else:
        merge = await asyncio.create_subprocess_exec(
            "gh", "pr", "merge", ref, "--merge",
            cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        await merge.communicate()
        if merge.returncode == 0:
            pull = await asyncio.create_subprocess_exec(
                "git", "pull", "--ff-only", cwd=cwd,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await pull.communicate()
            return True
        return False


def build_conflict_prompt(ref: str, base_branch: str, *, local: bool = False) -> str:
    """Build a prompt for a conflict-resolution worker."""
    if local:
        return f"""\
You are a conflict-resolution agent. A worker branch has merge conflicts
with the base branch that need to be resolved.

## Your branch

You are on branch `{ref}`. It needs to be rebased onto `{base_branch}`.

## Instructions

1. Rebase this branch onto the base branch:
   git rebase {base_branch}

2. If there are merge conflicts, resolve them:
   - Read the conflicting files to understand both sides
   - Resolve each conflict preserving the intent of both changes
   - `git add` resolved files
   - `git rebase --continue`
   - Repeat until the rebase is complete

3. Run the project's test suite to verify nothing is broken.
   Fix any test failures your conflict resolution introduced.

4. When done, verify: `git status` should show a clean working tree
   on the rebased branch, and `git log --oneline {base_branch}..HEAD`
   should show the branch's commits cleanly on top of {base_branch}.
"""
    else:
        return f"""\
You are a conflict-resolution agent. A PR has merge conflicts with the
base branch that need to be resolved.

## PR to fix

{ref}

## Base branch

`{base_branch}`

## Instructions

1. Check the current branch (it's already checked out for you):
   git log --oneline -5

2. Fetch and rebase onto the base branch:
   git fetch origin {base_branch}
   git rebase origin/{base_branch}

3. If there are merge conflicts, resolve them:
   - Read the conflicting files to understand both sides
   - Resolve each conflict preserving the intent of both changes
   - `git add` resolved files
   - `git rebase --continue`
   - Repeat until the rebase is complete

4. Run the project's test suite to verify nothing is broken.
   Fix any test failures your conflict resolution introduced.

5. Force-push the rebased branch to update the PR:
   git push --force-with-lease
"""


async def run_conflict_worker(
    state: SwarmState,
    worker: WorkerState,
    ref: str,
    *,
    model: str,
    budget: float,
    timeout: int,
    base_branch: str,
    local: bool = False,
) -> None:
    """Run a conflict-resolution worker in a worktree checked out to the conflicting branch."""
    worker.task = Task(id="merge", title=f"Resolve: {ref[:50]}", description="", status="in_progress")
    worker.result = "running"
    worker.start_time = time.monotonic()
    worker.conflict_ref = ref
    worker.pr_url = ""

    worktree_dir = state.repo / ".swarm" / "worktrees" / f"worker-{worker.slot}"
    worker.worktree = worktree_dir

    event(state, f"worker-{worker.slot} resolving conflicts on {ref[:60]}")

    # Determine branch name for worktree checkout
    if local:
        branch = ref
    else:
        # Extract branch name from PR URL
        pr_view = await asyncio.create_subprocess_exec(
            "gh", "pr", "view", ref, "--json", "headRefName", "-q", ".headRefName",
            cwd=state.repo, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await pr_view.communicate()
        branch = out.decode().strip()
        if not branch:
            event(state, f"worker-{worker.slot} could not determine PR branch for {ref}")
            worker.result = "failed"
            worker.conflict_ref = ""
            return
        # Ensure branch exists locally
        await asyncio.create_subprocess_exec(
            "git", "fetch", "origin", branch,
            cwd=state.repo, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )

    worker.branch = branch

    # Clean up stale worktree at this path
    if worktree_dir.exists():
        try:
            p = await asyncio.create_subprocess_exec(
                "git", "worktree", "remove", "--force", str(worktree_dir),
                cwd=state.repo, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await p.communicate()
        except Exception:
            pass

    # Create worktree from the conflicting branch
    proc = await asyncio.create_subprocess_exec(
        "git", "worktree", "add", str(worktree_dir), branch,
        cwd=state.repo, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        # For PR mode, the local branch might not exist — try from origin
        if not local:
            proc = await asyncio.create_subprocess_exec(
                "git", "worktree", "add", str(worktree_dir), f"origin/{branch}",
                cwd=state.repo, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
        if proc.returncode != 0:
            event(state, f"worker-{worker.slot} conflict worktree failed: {stderr.decode().strip()[:80]}")
            worker.result = "failed"
            worker.conflict_ref = ""
            return

    prompt = build_conflict_prompt(ref, base_branch, local=local)
    cmd = [
        "claude", "-p",
        "--output-format", "stream-json", "--verbose",
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
            env=_CLAUDE_ENV,
            limit=10 * 1024 * 1024,
        )
        worker.process = worker_proc

        def _on_line(text: str) -> None:
            worker.last_line = text

        if timeout > 0:
            try:
                await asyncio.wait_for(
                    stream_output(worker_proc, prompt.encode(), _on_line),
                    timeout=timeout * 60,
                )
            except asyncio.TimeoutError:
                worker_proc.kill()
                await worker_proc.wait()
                dur = time.monotonic() - worker.start_time
                event(state, f"worker-{worker.slot} conflict TIMEOUT after {fmt_elapsed(dur)}")
                worker.result = "timeout"
                return
        else:
            await stream_output(worker_proc, prompt.encode(), _on_line)

        dur = time.monotonic() - worker.start_time
        if worker_proc.returncode == 0:
            worker.result = "success"
            event(state, f"worker-{worker.slot} conflict resolved ({fmt_duration_short(dur)})")
        else:
            worker.result = "failed"
            event(state, f"worker-{worker.slot} conflict resolution failed ({fmt_duration_short(dur)})")

    except Exception as exc:
        worker.result = "failed"
        event(state, f"worker-{worker.slot} conflict exception: {exc}")
    finally:
        worker.process = None


# ── Worker ───────────────────────────────────────────────────────────────────

def build_worker_prompt(
    task: Task, slot: int, *,
    git_name: str, git_email: str, base_branch: str, local: bool = False,
    conventions: str = "", recent_tasks: list[str] | None = None,
) -> str:
    if local:
        finish_steps = f"""\
5. Close the beads task with the branch name:
   bd close {task.id} --reason "branch: $(git rev-parse --abbrev-ref HEAD)\""""
    else:
        finish_steps = f"""\
5. Push your branch:
   git push -u origin HEAD

6. Create a PR targeting the base branch:
   gh pr create --base {base_branch} --title "{task.title}" --body "Implements {task.id}: {task.title}"

7. Close the beads task with the PR URL:
   bd close {task.id} --reason "PR: <paste the PR URL here>\""""

    # Build optional context sections
    extra_sections = ""
    if conventions:
        extra_sections += f"""
## Conventions (MUST follow)

{conventions}
"""
    if recent_tasks:
        recent_list = "\n".join(f"  - {t}" for t in recent_tasks)
        extra_sections += f"""
## Recently completed by other workers (reuse existing code, don't reimplement)

{recent_list}
"""

    return f"""\
You are worker-{slot} in a swarm of AI coding agents. You have been assigned
a single task to implement.

## Your task

ID: {task.id}
Title: {task.title}

Description:
{task.description}
{extra_sections}
## Instructions

1. First, claim the task:
   bd update {task.id} --claim --actor worker-{slot}

2. Implement ONLY the change described above — nothing more.
   - Read relevant files first, understand the code.
   - Check what other workers have already built before writing new code.
   - Make the minimal, focused changes needed.
   - Follow existing code style and conventions.
   - Do NOT work on anything outside your assigned task.
   - Other modules are being handled by other workers.
   - If the task requires resources you don't have (GPU, cloud services, etc.),
     write the script/config but do NOT generate fake or placeholder results.
     Close the task with: bd close {task.id} --reason "NEEDS_HUMAN: <what's needed>"

3. Run the project's test suite. Fix any failures your changes introduced.

4. Commit with a clear message (use this exact git identity):
   git add -A
   git -c user.name="{git_name}-{slot}" -c user.email="{git_email}" commit -m "<descriptive message>"

{finish_steps}

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
    base_branch: str,
    local: bool = False,
    conventions: str = "",
    recent_tasks: list[str] | None = None,
) -> None:
    """Run a single worker in its worktree."""
    worker.task = task
    worker.result = "running"
    worker.pr_url = ""
    worker.start_time = time.monotonic()

    ts = f"{int(time.time())}-{int(time.monotonic() * 1000) % 10000}"
    branch = f"swarm/worker-{worker.slot}-{ts}"
    worktree_dir = state.repo / ".swarm" / "worktrees" / f"worker-{worker.slot}"
    worker.branch = branch
    worker.worktree = worktree_dir

    event(state, f"worker-{worker.slot} starting {task.id} \"{truncate(task.title, 40)}\"")

    # Clean up stale worktree at this path if it exists
    if worktree_dir.exists():
        try:
            p = await asyncio.create_subprocess_exec(
                "git", "worktree", "remove", "--force", str(worktree_dir),
                cwd=state.repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await p.communicate()
        except Exception:
            pass

    # Delete stale branch if it exists
    p = await asyncio.create_subprocess_exec(
        "git", "branch", "-D", branch,
        cwd=state.repo,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await p.communicate()

    # Create worktree (retry once after cleanup race)
    for attempt in range(2):
        proc = await asyncio.create_subprocess_exec(
            "git", "worktree", "add", "-b", branch, str(worktree_dir),
            cwd=state.repo,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            break
        if attempt == 0:
            # Cleanup race — prune and retry
            await asyncio.create_subprocess_exec(
                "git", "worktree", "prune",
                cwd=state.repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.sleep(0.5)
            # Re-delete branch in case prune freed it
            await asyncio.create_subprocess_exec(
                "git", "branch", "-D", branch,
                cwd=state.repo,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        else:
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

    prompt = build_worker_prompt(
        task, worker.slot, git_name=git_name, git_email=git_email,
        base_branch=base_branch, local=local,
        conventions=conventions, recent_tasks=recent_tasks,
    )
    cmd = [
        "claude", "-p",
        "--output-format", "stream-json", "--verbose",
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
            env=_CLAUDE_ENV,
            limit=10 * 1024 * 1024,
        )
        worker.process = worker_proc

        prompt_bytes = prompt.encode()

        def _on_line(text: str) -> None:
            worker.last_line = text

        if timeout > 0:
            try:
                output = await asyncio.wait_for(
                    stream_output(worker_proc, prompt_bytes, _on_line),
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
            output = await stream_output(worker_proc, prompt_bytes, _on_line)

        dur = time.monotonic() - worker.start_time

        # Check if the task was closed in beads
        try:
            task_data = await bd_json("show", task.id, cwd=state.repo)
            if isinstance(task_data, list) and task_data:
                task_data = task_data[0]
            task_status = task_data.get("status", "") if isinstance(task_data, dict) else ""
        except Exception:
            task_status = ""

        # Extract PR URL or branch from output/close_reason
        ref = ""
        if local:
            ref = branch  # we know the branch name
        else:
            for line in output.splitlines():
                if "github.com" in line and "/pull/" in line:
                    for word in line.split():
                        if "github.com" in word and "/pull/" in word:
                            ref = word.strip()
                            break
                    if ref:
                        break

        # If worker didn't close the bead, check if it made commits
        if task_status != "closed" and worktree_dir.exists():
            check_commits = await asyncio.create_subprocess_exec(
                "git", "rev-list", "--count", f"{base_branch}..HEAD",
                cwd=worktree_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            count_out, _ = await check_commits.communicate()
            commit_count = int(count_out.decode().strip() or "0")
            if commit_count > 0:
                reason = f"branch: {branch}" if local else f"auto-closed by orchestrator"
                await bd("close", task.id, "--reason", reason,
                         cwd=state.repo, check=False)
                task_status = "closed"
                event(state, f"worker-{worker.slot} had {commit_count} commits, auto-closed bead")

        # Check if worker flagged this as needing human intervention
        close_reason = ""
        if task_status == "closed" and isinstance(task_data, dict):
            close_reason = task_data.get("close_reason", "") or ""

        if task_status == "closed" and "NEEDS_HUMAN" in close_reason:
            worker.result = "success"
            state.completed += 1
            state.history.append(CompletedTask(
                task.id, task.title, worker.slot, dur, "success"))
            event(state, f"worker-{worker.slot} ⚠ {task.id} NEEDS_HUMAN ({fmt_duration_short(dur)})")
        elif task_status == "closed":
            worker.result = "success"
            worker.pr_url = ref
            state.completed += 1
            state.history.append(CompletedTask(
                task.id, task.title, worker.slot, dur, "success", ref))
            ref_note = f"  {ref}" if ref else ""
            event(state, f"worker-{worker.slot} ✓ {task.id} ({fmt_duration_short(dur)}){ref_note}")
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

async def verify_tools(*, local: bool = False) -> None:
    """Check that required tools are in PATH."""
    required = ["bd", "claude", "git"]
    if not local:
        required.append("gh")
    for tool in required:
        if not shutil.which(tool):
            print(f"Error: '{tool}' not found in PATH", file=sys.stderr)
            sys.exit(1)


async def ensure_beads(repo: Path) -> None:
    """Check that beads is working, initialize if needed."""
    beads_dir = repo / ".beads"
    if not beads_dir.exists():
        log("Initializing beads database...")
        await bd("init", cwd=repo)
        # Disable auto-backup to avoid noisy dolt commits
        config_path = repo / ".beads" / "config.yaml"
        if config_path.exists():
            text = config_path.read_text()
            if "backup:" not in text:
                text += "\nbackup:\n  enabled: false\n"
            else:
                text = re.sub(
                    r"(backup:\s*\n\s*enabled:\s*)true",
                    r"\1false",
                    text,
                )
            config_path.write_text(text)


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


# ── Design doc dependency parsing ────────────────────────────────────────────

def parse_design_deps(doc_path: Path) -> dict[int, list[int]]:
    """Parse dependency edges from a design doc.

    Supports two formats:
      1. Inline:  **PR 6 — Strategy evaluator loop** (depends on: PR 2, 3, 4)
      2. Multi-line (bold list item):
           **T14 — Track A read-only sidecar training**
           - **Depends on:** T5, T7, T12, T13

    Returns {6: [2, 3, 4], ...} or {14: [5, 7, 12, 13], ...}
    """
    text = doc_path.read_text()
    deps: dict[int, list[int]] = {}

    # Format 1: inline (depends on: ...) in the header line
    for m in re.finditer(
        r"\*\*(?:PR|T)\s*(\d+)\b.*?\(depends on:\s*(.+?)\)",
        text,
    ):
        task_num = int(m.group(1))
        dep_str = m.group(2)
        blockers = [int(d) for d in re.findall(r"(?:PR|T)\s*(\d+)", dep_str)]
        if not blockers:
            blockers = [int(d) for d in re.findall(r"(\d+)", dep_str)]
        if blockers:
            deps[task_num] = blockers

    # Format 2: **T<N> — title** followed by - **Depends on:** line
    current_task: int | None = None
    for line in text.splitlines():
        # Match task header: **T14 — ...** or **PR 3 — ...**
        hdr = re.match(r"\*\*(?:PR|T)\s*(\d+)\b", line)
        if hdr:
            current_task = int(hdr.group(1))
            continue
        # Match depends-on list item
        dep_match = re.match(r"-\s*\*\*Depends on:\*\*\s*(.+)", line, re.IGNORECASE)
        if dep_match and current_task is not None:
            dep_str = dep_match.group(1).strip()
            if dep_str.lower() in ("nothing", "none", "—", "-"):
                current_task = None
                continue
            blockers = [int(d) for d in re.findall(r"(?:PR|T)\s*(\d+)", dep_str)]
            if not blockers:
                blockers = [int(d) for d in re.findall(r"(\d+)", dep_str)]
            if blockers and current_task not in deps:
                deps[current_task] = blockers
            current_task = None

    return deps


async def apply_design_deps(
    epic_id: str,
    doc_path: Path,
    *,
    cwd: Path | None = None,
) -> int:
    """Parse deps from design doc and apply them to beads tasks.

    Maps "PR N" to bead IDs by matching task titles containing "PR N".
    Returns number of edges added.
    """
    deps = parse_design_deps(doc_path)
    if not deps:
        return 0

    # Build task number -> bead ID mapping from task titles
    # Matches "PR 3", "T14", "T1 —", etc. at the start of titles
    children = await get_all_children(epic_id, cwd=cwd)
    pr_to_bead: dict[int, str] = {}
    for c in children:
        title = c.get("title", "")
        m = re.match(r"(?:PR|T)\s*(\d+)\b", title)
        if m:
            pr_to_bead[int(m.group(1))] = c.get("id", "")

    added = 0
    for pr_num, blockers in deps.items():
        blocked_id = pr_to_bead.get(pr_num)
        if not blocked_id:
            continue
        for blocker_num in blockers:
            blocker_id = pr_to_bead.get(blocker_num)
            if not blocker_id:
                continue
            try:
                await bd("dep", "add", blocked_id, blocker_id, cwd=cwd)
                added += 1
            except Exception:
                pass  # already exists or other error
    return added


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
    pending_conflicts: list[str] = []  # refs needing conflict resolution
    conventions_text = args.conventions.read_text().strip() if args.conventions else ""
    recent_titles: list[str] = []  # titles of recently completed tasks
    assigned_ids: set[str] = set()  # task IDs currently assigned to workers
    planner_cooldown_until: float = 0  # monotonic time before which planner won't respawn

    # Start the display refresh loop
    display_task = asyncio.create_task(display_loop(state))

    if args.epic:
        # Resuming — skip planner, check existing tasks
        ready = await get_ready_tasks(state.epic_id, cwd=state.repo)
        all_children = await get_all_children(state.epic_id, cwd=state.repo)
        if not ready and not all_children:
            event(state, f"epic {args.epic} has no tasks — exiting")
            state.shutdown = True
            display_task.cancel()
            return
        event(state, f"resuming epic {args.epic}: {len(ready)} ready, {len(all_children)} total")
    else:
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

    # Apply deps from design doc if provided
    if args.design_doc:
        n = await apply_design_deps(
            state.epic_id, args.design_doc, cwd=state.repo)
        if n > 0:
            event(state, f"applied {n} dependency edges from {args.design_doc.name}")
            # Refresh ready tasks since deps may have changed what's unblocked
            ready = await get_ready_tasks(state.epic_id, cwd=state.repo)
            state.last_children = await get_all_children(state.epic_id, cwd=state.repo)
        else:
            event(state, f"no new deps found in {args.design_doc.name}")

    if args.dry_run:
        display_task.cancel()
        children = state.last_children
        sys.stderr.write(f"\n  {C.BOLD}Dry run — planner created {len(children)} tasks:{C.RESET}\n\n")
        for c in children:
            cid = c.get("id", "")
            title = c.get("title", "")
            status = c.get("status", "open")
            sys.stderr.write(f"  {cid}  {status:<12} {title}\n")
        # Show dependency info
        sys.stderr.write(f"\n  {C.BOLD}Dependencies:{C.RESET}\n\n")
        for c in children:
            cid = c.get("id", "")
            try:
                dep_data = await bd_json("dep", "list", cid, cwd=state.repo)
                # bd dep list returns a list of related issues with dependency_type
                blockers_list = []
                if isinstance(dep_data, list):
                    for item in dep_data:
                        if isinstance(item, dict) and item.get("dependency_type") == "blocks":
                            blockers_list.append(item.get("id", ""))
                elif isinstance(dep_data, dict):
                    blocked_by = dep_data.get("blocked_by", dep_data.get("blockedBy", []))
                    blockers_list = [
                        b.get("id", str(b)) if isinstance(b, dict) else str(b)
                        for b in blocked_by
                    ]
                if blockers_list:
                    sys.stderr.write(f"  {cid}  blocked by: {', '.join(blockers_list)}\n")
            except Exception:
                pass
        sys.stderr.write(f"\n  {C.DIM}epic: {state.epic_id}{C.RESET}\n")
        sys.stderr.write(f"  {C.DIM}Re-run without --dry-run to start workers.{C.RESET}\n\n")
        return

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
            if (not args.no_planner
                    and ready_count < refill_threshold
                    and not state.planner_running
                    and (planner_task is None or planner_task.done())
                    and time.monotonic() >= planner_cooldown_until):
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

                # Check if unmerged refs can unblock progress
                if not pending_conflicts:
                    try:
                        mergeable = await get_mergeable_refs(
                            state.epic_id, local=args.local, cwd=state.repo)
                    except Exception as exc:
                        log(f"Error checking mergeable refs: {exc}")
                        mergeable = []

                    if mergeable:
                        # Try simple merge first, queue failures for conflict workers
                        active_refs = {w.conflict_ref for w in state.workers if w.conflict_ref}
                        for ref in mergeable:
                            if ref in active_refs:
                                continue
                            merged = await try_merge_ref(
                                ref, base_branch=args.base_branch,
                                local=args.local, cwd=state.repo)
                            if merged:
                                event(state, f"merged {ref[:60]}")
                            else:
                                pending_conflicts.append(ref)
                        if pending_conflicts:
                            continue  # assign conflict workers in task assignment

                # If there are conflict workers still running, wait for them
                conflict_workers = [t for s, t in worker_tasks.items()
                                    if not t.done() and state.workers[s].conflict_ref]
                if conflict_workers:
                    event(state, f"waiting for {len(conflict_workers)} conflict workers")
                    await asyncio.wait(conflict_workers, return_when=asyncio.FIRST_COMPLETED)
                    continue

                event(state, "no tasks, no workers, planner idle — done")
                break

            # Assign tasks to idle worker slots (conflicts first, then regular tasks)
            ready_idx = 0
            for w in state.workers:
                if state.target > 0 and state.completed >= state.target:
                    break
                slot = w.slot
                if slot in worker_tasks and not worker_tasks[slot].done():
                    continue

                if slot in worker_tasks and worker_tasks[slot].done():
                    await cleanup_worktree(state, w)

                # Priority 1: conflict resolution
                if pending_conflicts:
                    ref = pending_conflicts.pop(0)
                    worker_tasks[slot] = asyncio.create_task(
                        run_conflict_worker(
                            state, w, ref,
                            model=args.merger_model,
                            budget=args.budget,
                            timeout=args.timeout,
                            base_branch=args.base_branch,
                            local=args.local,
                        )
                    )
                    continue

                # Priority 2: regular beads tasks
                while ready_idx < len(ready) and ready[ready_idx].id in assigned_ids:
                    ready_idx += 1
                if ready_idx >= len(ready):
                    break

                task = ready[ready_idx]
                ready_idx += 1
                assigned_ids.add(task.id)

                worker_tasks[slot] = asyncio.create_task(
                    run_worker(
                        state, w, task,
                        model=args.model,
                        budget=args.budget,
                        timeout=args.timeout,
                        git_name=args.git_name,
                        git_email=args.git_email,
                        base_branch=args.base_branch,
                        local=args.local,
                        conventions=conventions_text,
                        recent_tasks=recent_titles,
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
                        # Re-apply design doc deps to cover newly created tasks
                        elif args.design_doc:
                            n = await apply_design_deps(
                                state.epic_id, args.design_doc, cwd=state.repo)
                            if n > 0:
                                event(state, f"applied {n} dependency edges from {args.design_doc.name}")
                    except Exception as exc:
                        event(state, f"planner failed: {exc}")
                        planner_cooldown_until = time.monotonic() + 30
                        event(state, "planner cooldown 30s before retry")
                    planner_task = None
                else:
                    for slot, wt in worker_tasks.items():
                        if wt is t:
                            try:
                                t.result()
                            except Exception as exc:
                                event(state, f"worker-{slot} raised: {exc}")
                            w = state.workers[slot]
                            if w.task and w.task.id != "merge":
                                assigned_ids.discard(w.task.id)

                            if w.conflict_ref:
                                # Conflict worker finished — retry merge
                                ref = w.conflict_ref
                                w.conflict_ref = ""
                                if w.result == "success":
                                    merged = await try_merge_ref(
                                        ref, base_branch=args.base_branch,
                                        local=args.local, cwd=state.repo)
                                    if merged:
                                        event(state, f"merged {ref[:60]} after conflict resolution")
                                    else:
                                        event(state, f"merge still failing for {ref[:60]} — re-queuing")
                                        pending_conflicts.append(ref)
                                await cleanup_worktree(state, w)
                            elif w.result == "success":
                                # Track for recently-merged context
                                if w.task:
                                    recent_titles.append(w.task.title)
                                # Regular worker — auto-merge
                                merge_ref = w.branch if args.local else w.pr_url
                                if merge_ref:
                                    merged = await try_merge_ref(
                                        merge_ref, base_branch=args.base_branch,
                                        local=args.local, cwd=state.repo)
                                    if merged:
                                        event(state, f"merged {merge_ref[:60]}")
                                    else:
                                        event(state, f"merge failed for {merge_ref[:60]} — queuing conflict worker")
                                        pending_conflicts.append(merge_ref)
                            break

            # Periodically retry unmerged refs (simple merge first, then queue conflicts)
            if state.tick % 20 == 0:  # every ~10s
                active_refs = {w.conflict_ref for w in state.workers if w.conflict_ref}
                queued_refs = set(pending_conflicts)
                try:
                    unmerged = await get_mergeable_refs(
                        state.epic_id, local=args.local, cwd=state.repo)
                except Exception:
                    unmerged = []
                for ref in unmerged:
                    if ref in active_refs or ref in queued_refs:
                        continue
                    merged = await try_merge_ref(
                        ref, base_branch=args.base_branch,
                        local=args.local, cwd=state.repo)
                    if merged:
                        event(state, f"retry-merged {ref[:60]}")
                    else:
                        pending_conflicts.append(ref)

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
        "--epic", default=None,
        help="Resume from an existing epic ID (skip planner initial run)",
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
        "--merger-model", default=None,
        help="Claude model for conflict-resolution workers (default: same as --model)",
    )
    parser.add_argument(
        "--budget", type=float, default=0,
        help="Max USD per worker session, 0 = unlimited (default: 0)",
    )
    parser.add_argument(
        "--local", action="store_true", default=False,
        help="Local mode: workers commit to branches, orchestrator merges locally (no GitHub PRs)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Run planner only, print tasks and dependencies, then exit",
    )
    parser.add_argument(
        "--no-planner", action="store_true", default=False,
        help="Disable automatic planner runs (use with --epic when tasks are pre-created)",
    )
    parser.add_argument(
        "--design-doc", type=Path, default=None,
        help="Parse dependency edges from a design doc (looks for 'depends on: PR N' patterns)",
    )
    parser.add_argument(
        "--conventions", type=Path, default=None,
        help="File with conventions to inject into every worker prompt",
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
    if args.merger_model is None:
        args.merger_model = args.model

    global _log_file
    log_dir = repo / ".swarm"
    log_dir.mkdir(parents=True, exist_ok=True)
    _log_file = log_dir / "swarm.log"

    env_size = sum(len(k) + len(v) + 2 for k, v in _CLAUDE_ENV.items())
    log(f"env: {len(_CLAUDE_ENV)} vars, {env_size} bytes")
    log("Verifying tools...")
    await verify_tools(local=args.local)
    log("Ensuring beads...")
    await ensure_beads(repo)

    log(f'Starting swarm: "{args.task}"')
    log(f"  concurrency={args.concurrency}  target={args.target or 'unlimited'}"
        f"  timeout={args.timeout or 'none'}  model={args.model}  planner={args.planner_model}")

    # Detect the base branch (whatever branch we're on when swarm starts)
    base_branch_proc = await asyncio.create_subprocess_exec(
        "git", "rev-parse", "--abbrev-ref", "HEAD",
        cwd=repo, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    base_out, _ = await base_branch_proc.communicate()
    args.base_branch = base_out.decode().strip() or "main"
    log(f"Base branch: {args.base_branch}")

    if args.epic:
        epic_id = args.epic
        log(f"Resuming epic: {epic_id}")
    else:
        log("Creating epic...")
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
