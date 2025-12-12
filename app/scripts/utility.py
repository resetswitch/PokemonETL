# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

import psycopg2
from psycopg2 import sql
import logging.config
import yaml
from pathlib import Path
import os

from dotenv import load_dotenv
from psycopg2._psycopg import connection

load_dotenv()

# -------------------------------------------------------------------------
# SECTION: Set Up Logging
# -------------------------------------------------------------------------
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = yaml.safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
fname = Path(__file__).name.replace(".py", "")
logger = logging.getLogger(fname)

# -------------------------------------------------------------------------
# SECTION: Utility Functions
# -------------------------------------------------------------------------



def get_ddb_conn():
    """
    Creates a duckdb connection to interface with the database

    Returns
    -------
    conn : duckdb.Connection
    """
    import duckdb as ddb
    pg_uri = (
        f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}"
        f"@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}"
        f"/{os.getenv("POSTGRES_DB")}"
    )

    # 2️⃣  DuckDB: install/load extension, attach PG, run a query
    conn = ddb.connect()
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_uri}' AS pg (TYPE POSTGRES);")
    return conn

def is_connection_to_db_valid() -> bool:
    """
    Tests connection to database

    Returns
    -------
    bool
        if connection is valid then True, else False
    """
    connection_valid = False
    try:
        conn = get_connection_to_db()
        logger.info(f"connected to database")
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            logger.info(f"Version: {cur.fetchone()}")
            connection_valid = True
    except Exception as exc:
        logger.error("Connection Error to database", exc)
    finally:
        if "conn" in locals():
            conn.close()
            logger.info(f"closing connection")
    return connection_valid


def get_connection_to_db() -> psycopg2._psycopg.connection:
    """
    returns connection to database

    Returns
    -------
    psycopg2._psycopg.connection
        returns connection to database
    """

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")

    )
    return conn


if __name__ == "__main__":

    test = is_connection_to_db_valid()

