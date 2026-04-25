# ELT / Warehouse — dbt + Snowflake

## When to use
Snowflake is your warehouse. Team writes SQL. No distributed compute needed.

## How to run

```bash
pip install dbt-snowflake
cp .env.example .env && source .env
dbt deps
dbt debug          # verify connection
dbt run            # run all models
dbt test           # run schema tests
```
