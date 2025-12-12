## Project Overview  

The **PokémonETL** pipeline extracts data from the free public API[`pokeapi.co`](https://pokeapi.co/), normalizes it, and stores it in a relational PostgreSQL database.  

Key goals:

| Goal                             | How it’s achieved                                                                                                                                             |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Scalable extraction**          | Fully asynchronous HTTP calls with `aiohttp` + `tenacity` retry logic                                                                                         |
| **Idempotent loads**             | Upserts (`INSERT … ON CONFLICT`) guarantee repeatable runs                                                                                                    |
| **Clear separation of concerns** | - `db_schema.py` : DB creation & constraint handling  <br> - `etl.py` : per‑endpoint ETL logic  <br>- `utility.py` : connection pooling, helpers, progress UI |
| **Observability**                | Centralised `logging_config.yaml` with rotating file logs and console warnings                                                                                |
| **Reproducible environment**     | Docker‑Compose spins up PostgreSQL and the ETL container together                                                                                             |

---


## Architecture Diagram  
     
```
---------------          -----------------
| pokeapi.co  |          |   poke-etl    |
| (REST JSON) |◀------▶ |   (Python)    |
---------------          -----------------
                                ▲
                                |
                                ▼
                         -----------------
                         |   poke-db     |
                         | (PostgreSQL)  |
                         -----------------
                                ▲
                                |
                                |
                         -----------------
                         |  poke-jupyter |
                         |  (Jupyter)    |
                         -----------------
```
The Docker compose file also defines a poke-db service (PostgreSQL) and a poke-jupyter service (JupyterLab) that mounts the source code for interactive exploration.PrerequisitesToolMinimum versionWhyDocker Engine20.10+Runs the DB & ETL containersDocker Composev2.xOrchestrates multi‑service stackPython3.11 (only needed for local dev)Runs the scripts outside DockerGitanyClone the repoMake (optional)anyHandy shortcuts (see Makefile if you add one)

Note: All third‑party Python packages are listed in `requirements.txt`. They are installed automatically in the Docker image.

# Prerequisites
| Tool           | Minimum version                  | Why                              | 
|----------------|----------------------------------|----------------------------------|
| Docker Engine  | 20.10+                           | Runs the DB & ETL containers     | 
| Docker Compose | v2.x                             | Orchestrates multi‑service stack |
| Python         | 3.11 (only needed for local dev) | Runs the scripts outside Docker  |

Tested and developed with Linux Mint 22.1
Tested and working in Windows 11 (small bug with rich.progress output stream)

# Configuration (.env)
a `.env` file is already available, The most important variable is:

```
PROJECT_DIR_PATH_LOCAL=/Desired/Path/Here
```

# Running Locally (Docker)

```sh
# Build & start the stack
/PokemonETL$ docker compose build
/PokemonETL$ docker compose up
```

after `poke-etl` service completes the `poke-jupyter` service will go live
and allowing access to `dql.ipynb` (http://127.0.0.1:8899/lab/tree/app/scripts/dql.ipynb)
There you can query the database. `Shift` + `Enter` to execute the cells.

after you are done with everything. `CTRL`+`C` to close (if using linux)
```sh
docker compose down
```


# Source
https://pokeapi.co/docs/v2

