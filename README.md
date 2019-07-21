# DVCR: Development containers

Work with containers in your Python code.

## Installation

```bash
pip install dvcr
```

## Usage

A container can be started by simply creating an object. For instance we can set up postgres like so:

```python
>>> from dvcr.containers import Postgres
>>> postgres = Postgres()
[network]: Created network dvcr_network_8d8cfa3e
[postgres]: Pulling postgres:latest
[postgres]: Pulled image postgres:latest (sha256:4e045cb8eecd48ecf0b5eea966e9a8b0b9332b18e55d40a40f7971a1a0a08cb6)
[postgres]: Waiting for postgres ⏳
[postgres]: postgres is up! 🚀

```

We can now create tables and write data:
```python
>>> postgres.create_table(
...     schema="my_schema", table="my_table", columns=[("name", "VARCHAR(255)", "age" "INT")]
... )
[postgres]: CREATE SCHEMA IF NOT EXISTS my_schema;
[postgres]: CREATE SCHEMA
[postgres]: CREATE TABLE my_schema.my_table (name VARCHAR(255));
[postgres]: CREATE TABLE
>>> postgres.copy(schema="my_schema", table="my_table", path_or_buf="Speedy Ceviche,5\nPolly Esther,6\n")
[postgres]: COPY my_schema.my_table FROM STDIN DELIMITER ',';
```

Querying the table shows the desired result:
```bash
postgres=# SELECT * FROM my_schema.my_table;
      name       | age
-----------------+-----
 Speedy Ceviche  |  5
 Polly Esther    |  6
(2 rows)


```

Finally we can remove the container when we're done:
```python
>>> postgres.delete()
[postgres]: Deleted postgres ♻
[network]: Deleted network dvcr_network_8d8cfa3e ♻
```
