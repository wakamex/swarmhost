# swarmhost

A single-file Python orchestrator that runs N concurrent [Claude Code](https://code.claude.com/docs) workers against any codebase, coordinated through [beads](https://github.com/steveyegge/beadsbeads) (`bd`). Point it at a goal — "add test coverage", "migrate to TypeScript", "implement this design doc" — and it decomposes the work, manages dependencies, and produces a stream of commits or PRs while you sleep.

## The swarmhost concept

Most AI coding workflows are either **single-agent** (one Claude session doing everything sequentially) or **fan-out** (many agents working on independent tasks with no coordination). Swarmhost sits in between: it's a **host process** that maintains a swarm of AI workers with shared awareness of what's been done, what's in flight, and what's blocked.

The key insight is **separation of concerns across four roles**:

- The **orchestrator** (Python, no AI) manages processes, worktrees, and lifecycle — it never makes decisions about *what* to build
- The **planner** (Claude, spawned on-demand) has full codebase access and decides *what* to build next — it creates batches of well-scoped tasks
- The **workers** (Claude, one per task) each get an isolated git worktree and a single task — they implement, test, commit, and close
- The **merger** (Claude, shares the worker queue) resolves merge conflicts when the orchestrator's simple auto-merge fails — it gets a worktree checked out to the conflicting branch and rebases it

All coordination happens through beads, a local issue tracker. Every role is stateless — the planner, workers, and merger are each a fresh `claude -p` invocation, and the orchestrator keeps no persistent state of its own. Kill it and restart, and it picks up where it left off by querying beads. Each worker is isolated in its own git worktree, so concurrent agents never step on each other's files.

```
┌─────────────────────────────────────────────────────────────────┐
│  swarm.py  (asyncio, ~1500 lines, zero dependencies)            │
│                                                                 │
│  ┌──────────┐     ┌──────────────┐     ┌───────────────────-─┐  │
│  │ Planner  │────>│  beads (bd)  │<────│  Workers (1..N)     │  │
│  │ (claude) │     │              │     │  (claude per task)  │  │
│  │          │     │  epic        │     │                     │  │
│  │ creates  │     │   ├ task ✓   │     │  claim → implement  │  │
│  │ tasks    │     │   ├ task ⚙   │     │  → test → commit    │  │
│  │ when     │     │   ├ task ○   │     │  → PR/merge →       │  │
│  │ queue    │     │   ├ task ○   │     │  close              │  │
│  │ is low   │     │   └ ...      │     │                     │  │
│  └──────────┘     └──────────────┘     └───────────────────-─┘  │
│       ▲                                         │               │
│       │              Orchestrator               │               │
│       └── triggers when ready_count < threshold ┘               │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

The following must be in `PATH`:

| Tool | Purpose |
|---|---|
| [`bd`](https://github.com/anthropics/beads) | Task coordination (issue tracker) |
| [`claude`](https://docs.anthropic.com/en/docs/claude-code) | Claude Code CLI (workers, planner, merger) |
| `git` | Version control, worktree isolation |
| [`gh`](https://cli.github.com/) | GitHub CLI — only needed without `--local` |

Python 3.10+. No pip dependencies.

## Quick start

```bash
# Clone into any location — swarm.py runs against a separate target repo
git clone https://github.com/wakamex/swarmhost.git
cd swarmhost

# Run against a project (creates PRs on GitHub)
python swarm.py "add comprehensive test coverage" --repo /path/to/project -c 5

# Run locally (no PRs, merges branches directly)
python swarm.py "migrate from moment to date-fns" --repo /path/to/project -c 5 --local

# Dry run — see what the planner would create, then exit, resume with --epic
python swarm.py "add type annotations" --repo /path/to/project --dry-run
```

## Usage

```
python swarm.py <goal> [flags]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `goal` (positional) | required | High-level objective (or use `--goal-file`) |
| `--goal-file` | — | Read the goal from a file instead of CLI arg |
| `-c, --concurrency` | 5 | Max parallel workers (0 = unlimited) |
| `-n, --target` | 0 | Total tasks to complete (0 = until planner says done) |
| `--repo` | cwd | Target git repo path |
| `--epic` | — | Resume from an existing beads epic ID |
| `--timeout` | 0 | Per-worker timeout in minutes (0 = no limit) |
| `--refill` | concurrency | Ready-task threshold that triggers the planner |
| `--model` | sonnet | Claude model for workers |
| `--planner-model` | opus | Claude model for the planner |
| `--merger-model` | same as `--model` | Claude model for conflict-resolution workers |
| `--budget` | 0 | Max USD per worker session (0 = unlimited) |
| `--local` | false | Skip GitHub PRs; commit locally, merge onto base branch |
| `--no-planner` | false | Disable automatic planner refills (use with `--epic`) |
| `--design-doc` | — | Parse dependency graph from a design doc |
| `--conventions` | — | File with conventions injected into every worker prompt |
| `--dry-run` | false | Run planner, show tasks and deps, exit |
| `--git-name` | Swarm | Git author name for worker commits |
| `--git-email` | swarm@localhost | Git author email for worker commits |

## How it works

### Bootstrap

1. Verifies `bd`, `claude`, `git` (and `gh` if not `--local`) are in PATH
2. Runs `bd init` if no `.beads/` directory exists in the target repo
3. Creates an epic: `bd create "<goal>" -t epic`
4. Runs the planner synchronously to create the first batch of tasks
5. Optionally applies dependency edges from `--design-doc`
6. Enters the main loop

### Main loop

```
while not done:
    ready = tasks with no unresolved blockers

    if ready_count < threshold and planner not running:
        spawn planner in background

    if ready_count == 0 and no workers running:
        if planner running: wait for it
        if unmerged refs: try simple merge, queue failures for conflict workers
        else: exit (nothing left)

    for each idle worker slot:
        if pending conflict: assign conflict-resolution worker (priority)
        else: assign next ready task
        create isolated git worktree
        spawn worker in background

    on worker finish:
        auto-close bead if worker made commits but forgot
        auto-merge (rebase+ff in --local, gh pr merge in PR mode)
        on merge failure: queue for conflict-resolution worker
        cleanup worktree

    wait for any worker/planner to finish
```

### Worker isolation

Each worker runs in its own [git worktree](https://git-scm.com/docs/git-worktree), branching from the current HEAD of the base branch. Workers never touch each other's files. On completion, the orchestrator auto-merges the result — either via `git merge` locally or `gh pr merge` on GitHub — so the next worktree always starts from up-to-date code.

```
repo/
├── .swarm/
│   └── worktrees/
│       ├── worker-0/   ← isolated checkout
│       ├── worker-1/   ← isolated checkout
│       └── worker-2/   ← isolated checkout
└── (main repo, read-only during swarm)
```

### Dependency-aware execution

Tasks can have dependency edges (`bd dep add`). The orchestrator only assigns tasks whose blockers are all closed. The `--design-doc` flag parses these automatically from a design document:

```markdown
**PR 3 — Observer backends** (depends on: PR 1, PR 2)
```

Workers building on predecessor code get worktrees branched from HEAD *after* those predecessors have been merged, so they always start from a codebase that includes the work they depend on.

### Live status display

The orchestrator streams `--output-format stream-json` from each Claude subprocess and parses tool calls into human-readable status lines:

```
  swarm  "Implement the plan in DESIGN.md exactly as specified."
  epic   veda-ds-27c  |  25m14s

  ████████████████████████░░░░░░ 80%  8 done  1 running  1 queued
  avg 5m48s/task  19.0 tasks/hr  8 ok  5 fail
  eta 6m18s  ~8:55 PM ET

  workers
  ⠙ worker-0  ⚙ veda-ds-27c.4   "Reaction types"           12m
    Edit: types.py
  ⠙ worker-1  ⚙ veda-ds-27c.5   "Post-execution queue"      8m
    $ python -m pytest
  ⠙ worker-2  ⚙ veda-ds-27c.2   "YAML config loader"        3m
    Read: config.py
  ✓  worker-3  ⚙ veda-ds-27c.3  "Observer backends"         swarm/worker-3-1772674071-2814

    planner  idle  (0 runs)

  recent
  [20:40:14] worker-1 starting veda-ds-27c.6 "PR 6: Strategy evaluator loop — poll, o…"
  [20:42:43] worker-0 ✓ veda-ds-27c.8 (6m48s)  swarm/worker-0-1772674554-5630
```

### Two-mode operation

In both modes, the orchestrator auto-merges completed work immediately so successor workers always branch from a codebase that includes their dependencies. On merge failure, the orchestrator queues a conflict-resolution worker — a regular worker slot given a rebase-and-resolve prompt instead of a beads task.

**PR mode** (default): Workers push branches and create GitHub PRs via `gh pr create`. The orchestrator auto-merges each PR via `gh pr merge --merge` and pulls the result locally. If the merge fails (conflicts, CI checks, branch protection), a conflict-resolution worker rebases the PR branch and force-pushes the fix.

**Local mode** (`--local`): Workers commit to local branches. The orchestrator auto-merges via `git rebase` + `git merge --ff-only`. On merge conflicts, a conflict-resolution worker rebases the branch and resolves them. No GitHub interaction needed — useful for monorepos, private work, or fast iteration.

### Graceful shutdown

`Ctrl-C` sets a drain flag — in-flight workers finish their current task, no new workers are spawned. A second `Ctrl-C` force-kills everything immediately.

## State and recovery

All task state lives in beads. The orchestrator is stateless and can be restarted at any time:

```bash
# See full state even if the orchestrator crashed
bd list --parent <epic-id> --all

# Resume from where you left off
python swarm.py "same goal" --epic <epic-id> --no-planner --repo /path/to/project
```

Logs are written to `.swarm/swarm.log` in the target repo.

## Design doc workflow

For structured work (e.g., implementing an architecture from a design document), you can pre-plan the dependency graph:

```bash
# 1. Dry run: planner creates tasks, design-doc wires deps
python swarm.py "implement DESIGN.md" --repo /path/to/project \
  --design-doc DESIGN.md --dry-run

# 2. Review the task list and deps
bd list --parent <epic-id> --all

# 3. Run for real, skipping re-planning
python swarm.py "implement DESIGN.md" --repo /path/to/project \
  --epic <epic-id> --no-planner --design-doc DESIGN.md -c 4
```

## License

MIT
