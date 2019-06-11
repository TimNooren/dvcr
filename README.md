# DVCR - development containers

DVCR allows you to start up a different docker containers to use in tests.

## Installation

```bash
pip install dvcr
```

## Usage

A container can be started by instantiating the respective class:

```python
from dvcr.containers import Postgres

postgres = Postgres().wait()
```