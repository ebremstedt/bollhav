# When not to use bollhav

Bollhav has a narrow shape on purpose: Python models, interval batches, a
handful of backends, one process. That shape is right for a lot of pipelines
and wrong for some. Here are the cases where you should reach for something
else, so you do not find out the slow way.

## Your warehouse is not Postgres, SQL Server or Iceberg

Targets can be Postgres, SQL Server or Iceberg tables and views, nothing
else (see [Backends](../concepts/backends.md)). State lives in Postgres only,
so even an Iceberg or SQL Server pipeline needs a Postgres for the state,
library and error tables (see [State](../model/state.md)). If your data is in
BigQuery, Snowflake, Databricks, Redshift, ClickHouse or DuckDB, there is no
backend to write to, and none planned. Use the warehouse's own tooling, with
dbt or SQLMesh on top.

## The transformation belongs in SQL, inside the warehouse

Bollhav pulls rows out through Python and writes them back. When the data is
already in a warehouse and the team thinks in SQL, that is the wrong
direction: an extra worker, network in both directions, and logic that the
warehouse would run faster on its own. dbt and SQLMesh give SQL models,
tests, documentation and column-level lineage parsed from the queries.
Bollhav's lineage is what the models declare as upstreams, not what the SQL
says (see [Lineage](../articles/lineage.md)), and it has no test framework
beyond what you write in the execute function.

## The data is big, or the joins are

A model runs in one Python process: generators of dataframes, read in
chunks of 20,000 rows by default and at most 100,000, written with COPY and
merged through a staging table (see [Batch](../model/batch.md) and
[Write modes](../model/modes.md)). That is comfortable for millions of rows
per model per day on one machine. It is not a tool for joins across
terabytes, shuffles, or anything that needs a cluster. That is Spark, Trino,
or the warehouse's engine.

## You need events, not intervals

The unit of work is a time chunk cut by a cron expression, and late data is
handled by a lookback over recent chunks (see
[Temporality](../model/temporality.md)). That is batch, down to minutes if
you like. It is not change data capture, not a stream, and gives no ordering
or exactly-once guarantees between source and target. If the requirement is
seconds, or every change event, use Kafka, Debezium or Flink and let bollhav
model the tables they land in, if at all.

## You need an orchestrator's operations

Choreography is a state table and a loop (see
[Orchestration](../articles/orchestration.md)): models ask state what is
missing and run it. There is no scheduler UI, no sensors, no dependencies on
things outside bollhav, no SLAs, no access control, and notifications go to
Microsoft Teams only (see [Notifications](../model/notifications.md)). When
you have many kinds of jobs, people on call, audit requirements, or
dependencies across teams and systems, you need Airflow or Dagster. Bollhav
runs fine as a task inside them; it does not replace them.

## Most of the work is connectors

Every source is Python you write and maintain, including the pagination,
retries and schema drift of each API. There is no connector catalogue. A
team whose job is mostly landing forty SaaS sources should buy that:
Fivetran, Airbyte, or dlt in Python. Bollhav earns its keep in what happens
after landing.

## The team does not write Python

Models and execute functions are Python, 3.11 or later, on polars and
psycopg. Analysts who work in SQL, notebooks or a low-code tool will not be
productive here, and the framework will not meet them halfway.

## It is one script

Two nightly jobs from one source do not need a model object, state tables
and tag expressions. A cron and a script, or a scheduled GitHub Action, is
less to understand and less to break. Bollhav starts paying off at a dozen
models that share a shape.

## You need a company behind it

One person maintains bollhav. It has a small user base, major versions have
broken things (2.0, 3.0), and there is no support contract. If your
procurement needs a vendor, an SLA, or a community you can hire from, that
is a real reason to pass, and no amount of good design changes it.

## When it is the right tool

A team that writes Python, lands and transforms data in Postgres, SQL Server
or Iceberg, has many models of the same shape, and runs them in intervals of
minutes to days. Then the standard model object, the state, the tags and
the virtual environments give you more than you would build yourself, and
the metadata comes free.
