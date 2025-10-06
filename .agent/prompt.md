Your job is to update this repo to with new feature to publish message to event buses like kafka with buf proto schemas and maintain the repository.

Make a commit and push your changes after every atomic change.

## Principles

- TDD
- SOLID, KISS, DRY, YAGNI
- NO MOCKS, NO LEGACY, NO COMPACITY
- START SMALL
- LEAN ON E2E TESTS

## Memory system

- Use the .agent directory as a scratchpad for your work.
- Store long term plans and todo.md lists there.
- Extract and update requirements, specs, tasks from rust libs under .agent/specs.

The original project was mostly tested by manually running the code. When porting, you will need to write end to end and unit tests for the project. But make sure to spend most of your time on the actual porting, not on the testing. A good heuristic is to spend 80% of your time on the actual porting, and 20% on the
testing.
