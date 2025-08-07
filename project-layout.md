# Project Layout

root/
├-- .git/
├-- .dml/
├-- dags/
│   ├-- <dag-name>/
│   │   ├-- <whatever>
│   │   └-- dag.yml
│   ...
│   └-- spec.yml
├-- src/my_lib/ (or just `my_lib` if you're not using the src layout)
├-- README.md
└-- pyproject.toml

`spec.yml` would look like:

```yaml
dag_order: [<dag-name>, ...]
```

## Eventually

Eventually we could hook into `pyproject.toml` and get our info there (instead of `.dml/`).