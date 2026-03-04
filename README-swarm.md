# Swarm Orchestrator

A single-file Python 3.10+ asyncio orchestrator that runs N concurrent Claude workers against a codebase, coordinated through [beads](https://github.com/anthropics/beads) (`bd`). A planner agent refills the task queue when it runs low, giving each planning round full context of what's been done and what's in flight.

## Architecture: Three Roles

```
┌─────────────────────────────────────────────────────────────────┐
│  swarm.py  (Python, asyncio)                                   │
│                                                                 │
│  ┌──────────┐     ┌──────────────┐     ┌────────────────────┐  │
│  │ Planner  │────>│  beads (bd)  │<────│  Workers (1..N)    │  │
│  │ (claude) │     │              │     │  (claude per task)  │  │
│  │          │     │  epic        │     │                     │  │
│  │ creates  │     │   ├ task ✓   │     │  claim -> implement │  │
│  │ tasks    │     │   ├ task ⚙   │     │  -> test -> PR ->   │  │
│  │ when     │     │   ├ task ○   │     │  close              │  │
│  │ queue    │     │   ├ task ○   │     │                     │  │
│  │ is low   │     │   └ ...     │     │                     │  │
│  └──────────┘     └──────────────┘     └────────────────────┘  │
│       ^                                        │               │
│       │              Orchestrator               │               │
│       └── triggers when ready_count < threshold ┘               │
└─────────────────────────────────────────────────────────────────┘
```

**Orchestrator** (Python, ~770 lines): Stateless process manager. No AI. Manages the worker pool, monitors beads, triggers the planner when needed, tracks progress, handles worktree lifecycle.

**Planner** (Claude, spawned on-demand): Reads the full beads state + explores the codebase. Creates the next batch of 8-15 PR-worthy tasks under the epic. Has full context of what's done, what's in progress, what's queued. Signals `SWARM_GOAL_COMPLETE` when no more work remains.

**Worker** (Claude, one per task): Gets a specific task pre-assigned by the orchestrator. Claims it in beads, implements in an isolated git worktree, tests, commits, pushes, creates a PR, closes the beads task.

## Prerequisites

The following must be in `PATH`:

- `bd` — beads issue tracker
- `claude` — Claude Code CLI
- `git`
- `gh` — GitHub CLI (authenticated)

## Usage

```bash
python swarm.py "add comprehensive test coverage" --concurrency 5
python swarm.py "migrate from moment to date-fns" -c 5 -n 30
python swarm.py "add type annotations everywhere" -c 3 --repo /path/to/project
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `task` (positional) | required | The high-level goal |
| `-c, --concurrency` | 5 | Max parallel workers |
| `-n, --target` | 0 | Total tasks to complete (0 = until planner says done) |
| `--repo` | cwd | Git repo path |
| `--timeout` | 45 | Per-worker timeout in minutes |
| `--refill` | concurrency | Ready-queue threshold that triggers planner |
| `--model` | sonnet | Claude model for workers |
| `--planner-model` | opus | Claude model for planner |
| `--budget` | 2.00 | Max USD per worker session |

## How It Works

### Bootstrap

1. Verifies `bd`, `claude`, `git`, `gh` are in PATH
2. Runs `bd init` if no `.beads/` directory exists
3. Creates an epic: `bd create "<goal>" -t epic --json`
4. Runs the planner synchronously to create the first batch of tasks
5. Enters the main loop

### Main Loop

```
while not shutdown:
    ready = bd ready --parent <epic>

    if ready_count < threshold and planner not running:
        spawn planner in background

    if ready_count == 0 and no workers running:
        if planner running: wait for it
        else: exit (nothing left)

    for each idle worker slot:
        assign next ready task
        create git worktree
        spawn worker in background

    wait for any worker or planner to finish
    update display
```

### Worktree Lifecycle

1. Orchestrator creates: `git worktree add -b swarm/worker-N-<timestamp> .swarm/worktrees/worker-N`
2. Worker runs in the worktree, commits, pushes
3. On success: orchestrator removes worktree
4. On failure: worktree kept for inspection

### Shutdown

`SIGINT` or `SIGTERM` sets `shutdown = True`. The orchestrator waits 60 seconds for in-flight workers to finish, then cancels any remaining.

## State and Recovery

All state lives in beads. The orchestrator itself is stateless and could be restarted at any time. Run `bd list --parent <epic> --all` to see full state, even if the orchestrator crashed.

### Progress Display

Refreshed after each event:

```
  swarm: "add comprehensive test coverage"
  epic:  bd-a1b2  |  done: 12  running: 5  queued: 8  |  elapsed: 2h13m

  worker-0  ⚙ bd-c3d4  "Add tests for auth/login.py"          14m
  worker-1  ⚙ bd-e5f6  "Add tests for api/handlers.py"          8m
  worker-2  ⚙ bd-a7b8  "Add tests for utils/validation.py"      3m
  worker-3  ✓ bd-f1a2  "Add tests for models/user.py"      PR #42
  worker-4  ⚙ bd-b9c0  "Add edge cases for parser"              1m

  planner   idle
```

### Logs

Written to `.swarm/swarm.log` and stderr.
