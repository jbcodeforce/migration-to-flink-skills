# Confluent Cloud Tools

Shared Confluent Cloud Flink helpers used by pipeline CLIs and by `flink-skill-common`
(statement create/wait/delete, classify, drop, queries). Library entry points live in
`cc_deploy.statement_lifecycle` and `cc_deploy.flink_deploy`.

## Build flink pipeline manifest

The manifest folder includes a CLI to create a manifest file to help deploying flink statements for a set of related statements in a folder.

```sh
./scripts/create-manifest.sh --sql-dir path/to/demo
./scripts/create-manifest.sh --sql-dir path/to/demo --dry-run
./scripts/create-manifest.sh --sql-dir path/to/demo --overwrite
```


## Deploy Related Flink Statements using manifest

```sh
./scripts/deploy-flink-statements.sh --sql-dir path/to/demo groups
./scripts/deploy-flink-statements.sh --sql-dir path/to/demo deploy --group all
./scripts/deploy-flink-statements.sh --sql-dir path/to/demo undeploy --group all
./scripts/deploy-flink-statements.sh --sql-dir path/to/demo drop-tables
```

