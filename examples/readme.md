# gabsurd Examples

Self-contained, runnable Gleam projects demonstrating workflow patterns with the gabsurd SDK.

| Example | Pattern | Key Concepts |
|---------|---------|-------------|
| [`order_workflow/`](order_workflow/) | Full order processing pipeline | `context.step`, data passing between steps, event-driven suspend/resume, idempotent spawning |

## Running

All examples share the same PostgreSQL database used by the main project. Start it first:

```bash
mise run db
```

Then use the example mise tasks from the repo root:

```bash
mise run example-build   # compile the example
mise run example-format  # format the example source
mise run example-check   # check formatting + build
mise run example-run     # reset DB, then run the example end-to-end
```

See each example's `readme.md` for details on what it demonstrates.
