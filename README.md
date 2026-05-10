# E2E_ETL_Orchestration

## Airflow Snowflake Connection Provisioning

This project uses Docker Compose to provision the default Airflow Snowflake connection automatically from environment variables.

The compose service sets:

- `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`

which resolves to a Snowflake connection URI in the format:

```
snowflake://<user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>&role=<role>
```

Make sure the following env vars are defined before starting the stack:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_ROLE`

This eliminates the need to manually create `snowflake_default` inside Airflow.
