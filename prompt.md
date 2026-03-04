I want to run an overnight swarm (5 concurrent Claude workers) to improve test coverage across this codebase. Help me write a good goal string for this command:

python /code/clod-re/swarm.py "<GOAL>" -c 5 --repo /code/frame-modernized

Look at our existing test structure under test/ — the patterns, how mocks work (especially Valtio mocking and the componentSetup.js helpers), the naming conventions. Then look at what's NOT tested.
Major known gaps:

Main process (test with --env=node):
- main/reveal/, main/rpc/, main/ens/, main/txHistory/, main/crypt/, main/launch/, main/menu/, main/gasAlerts/, main/chainlist/

App views (test with --env=jsdom, use test/componentSetup.js):
- Almost all views under app/views/ are untested — only Accounts has a single test

Resources:
- resources/Components/Dropdown/, resources/Components/Cluster/, resources/domain/nav/, resources/domain/request/

Also look at the testing lapses we recently discovered and fixed — study what went wrong there (missing coverage that let bugs slip through, untested state transitions, edge cases we missed) and
make sure the planner creates tasks that address similar gaps across the rest of the codebase. The goal isn't just "add tests where there are none" but "add the tests that would have caught the
kinds of bugs we've been finding."

Each task should be a meaningful PR — not a single assertion, but a full test file with multiple describe/it blocks covering the happy path, edge cases, and error handling for that module. Think
"complete test coverage for main/ens/" not "add one test for ENS resolution". If a module is large (like main/rpc/ or main/provider/), the planner should split it into logical chunks (e.g. "test
RPC connection lifecycle" vs "test RPC error handling and reconnection") rather than one giant file or one trivial test.

Write me a goal string that:
1. Tells the planner to use Jest 29 with our existing test patterns
2. Specifies `npm run test:unit` to verify (not just the file in isolation)
3. Points at the gaps above but also lets the planner discover others
4. Says each task = one coherent test file scoped as a single reviewable PR
5. Prioritizes the kinds of coverage that catch real bugs — state transitions, error paths, boundary conditions — over trivial getter/setter tests
6. Mentions our key test infrastructure: test/componentSetup.js for React, test/setupGlobals.js, jest.mock + valtio snapshot/subscribe mocking pattern

Make it specific enough to produce good PRs from round 1 but not so rigid that the planner can't adapt. One paragraph, no line breaks.