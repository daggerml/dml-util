# DaggerML Funk Execution Flow

This document describes the complete execution flow of funkified functions in DaggerML, from decoration to result computation.

## Overview

DaggerML uses a multi-stage execution model where functions are converted to resources, then nodes, and finally executed through a CLI-based execution engine with caching and adapter support.

## Execution Flow Diagrams

### Function calling
`dag._put(resource)`
       ↓
   returns a ResourceNode (callable)
       ↓
`dag.fn(*args)`
       ↓
   returns a Node

Internally, `dag.fn(...)` runs `Repo.start_fn` in a loop (via the cli). That looks like:

```
       ┌────────────────────────────┐
       │ CLI: dml invoke start_fn   │
       │                            │
       │ 1. Insert args as nodes    │
       │ 2. Create argv datum       │
       │ 3. Check adapter           │
       └─────────────┬──────────────┘
                     ↓
┌─────────────────┐     ┌───────────────────────────────────────────┐
│ adapter == None │     │ adapter != None                           │
│                 │     │                                           │
│ Execute builtin │     │ Call cache_db.submit(fn, argv_datum.id,   │
│ directly in CLI │     │                       self.dump_ref(...)) │
└─────────────────┘     └─────────────────────┬─────────────────────┘
                                              ↓
                        ┌───────────────────────────────────────────┐
                        │ Cache.submit(fnresource, cache_key, dump) │
                        │                                           │
                        │ 1. Check cache                            │
                        │    ├─ Hit: return dag_dump/error_dump     │
                        │    └─ Miss: continue                      │
                        │ 2. Call adapter (with uri, data, etc.)    │
                        │ 3. If adapter prints result, cache it     │
                        │ 4. Return with result or None             │
                        └────────────────────┬──────────────────────┘
                                             ↓
                  ┌───────────────────────────────────────────────────────┐
                  │ Cache.submit returns to `start_fn` with               │
                  │   whatever the adapter returned with                  │
                  │   (possibly cached)                                   │
                  │                                                       │
                  │ `start_fn` loads the ref into the dag and returns (or │
                  |   raises) if `cache.submit` returned non null value.  │
                  └──────────────────────────┬────────────────────────────┘
                                             ↓
                       ┌─────────────────────────────────────────────┐
                       │ CLI processes cache.submit result:          │
                       │                                             │
                       │ If result is not None:                      │
                       │   insert the node and return (or raise)     │
                       └─────────────────────────────────────────────┘
```

### `funkify`

@funkify decorator
       ↓
   transforms a python function into a `Resource`

### Adapters and Runners (in `dml-util`).


```
+-------------------+
| AdapterBase.cli() |
+-------------------+
          |
          v
+-----------------------------+
| Parse CLI args & config     |
+-----------------------------+
          |
          v
+-----------------------------+
| Setup logging               |
+-----------------------------+
          |
          v
+-----------------------------+
| Read input data (file/stdin/|
| S3) via _read_data          |
+-----------------------------+
          |
          v
+-----------------------------+
| For n_iters:                |
|   |                         |
|   v                         |
| Call send_to_remote(uri,    |
|   config, dump)             |
|   |                         |
|   v                         |
| (resp, msg) = ...           |
|   |                         |
|   v                         |
| Write msg to error output   |
|   |                         |
|   v                         |
| If resp:                    |
|   Write resp to output      |
|   Exit 0                    |
| Else:                       |
|   n_iters -= 1              |
|   sleep(0.2)                |
+-----------------------------+
          |
          v
+-----------------------------+
| On Exception:               |
|   Log error                 |
|   Try to write serialized   |
|   error to output           |
|   Exit 0 or 1               |
+-----------------------------+
          |
          v
+-----------------------------+
| Teardown logging handlers   |
+-----------------------------+
```

send_to_remote dispatches to:
    |-- LocalAdapter: resolves runner, calls runner.run()
    |-- LambdaAdapter: invokes AWS Lambda, parses response
    |-- (other adapters: similar pattern)
