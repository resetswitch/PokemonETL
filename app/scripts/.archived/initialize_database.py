import sys

import psycopg2
import logging.config
import yaml
from pathlib import Path
import os
import sys

from dotenv import load_dotenv, dotenv_values # importing necessary functions from dotenv library
load_dotenv()

# SECTION: Set Up Logging
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = yaml.safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
logger = logging.getLogger("initialize_database")


# SECTION: Connect to PostgreSQL
# Connect to an existing database (e.g., the default 'postgres' DB)
logger.info(f"connecting to database")

try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),  # change if you connect to another DB
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    print("✅ Connected! PostgreSQL version:")
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        print(cur.fetchone())
finally:
    if "conn" in locals():
        conn.close()




# SECTION: Recreating the Connection (due to priveledges)

db_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),      # change if you connect to another DB
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
db_cur = db_conn.cursor()


# SECTION: Creating the Table
create_table_sql_pokemon_species= """
CREATE TABLE IF NOT EXISTS pokemon_species (
    id SMALLSERIAL PRIMARY KEY,
    species TEXT,
    generation SMALLSERIAL
);
"""

create_table_sql_pokemon_flavor_texts= """
DROP TABLE IF EXISTS pokemon_flavor_texts;
CREATE TABLE IF NOT EXISTS pokemon_flavor_texts(
    id        SMALLSERIAL NOT NULL,
    text      TEXT,
    version   TEXT,

    PRIMARY KEY (id, version),

    CONSTRAINT pk_pokemon_flavor_texts_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

create_table_sql_pokemon_moves= """
DROP TABLE IF EXISTS pokemon_moves;
CREATE TABLE IF NOT EXISTS pokemon_moves(
    id        SMALLSERIAL NOT NULL,
    move      TEXT,
    level_learned_at   SMALLSERIAL,
    learn_method  TEXT,
    sort_order SMALLSERIAL,
    version_group TEXT,

    PRIMARY KEY (id, move, level_learned_at, learn_method, sort_order, version_group),

    CONSTRAINT pk_pokemon_moves_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

create_table_sql_pokemon_abilities= """
DROP TABLE IF EXISTS pokemon_abilities;
CREATE TABLE IF NOT EXISTS pokemon_abilities (
    id        SMALLSERIAL NOT NULL,
    slot      SMALLSERIAL NOT NULL,
    ability   TEXT,
    is_hidden BOOL,

    PRIMARY KEY (id, slot),

    CONSTRAINT pk_pokemon_abilities_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

create_table_sql_pokemon_types= """
DROP TABLE IF EXISTS pokemon_types;
CREATE TABLE IF NOT EXISTS pokemon_types (
    id      SMALLSERIAL NOT NULL,
    slot    SMALLSERIAL NOT NULL,
    type    TEXT NOT NULL,

    PRIMARY KEY (id, type),

    CONSTRAINT pk_pokemon_types_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

create_table_sql_pokemon_base_stats = """
DROP TABLE IF EXISTS pokemon_base_stats;
CREATE TABLE IF NOT EXISTS pokemon_base_stats (
    id      SMALLSERIAL NOT NULL,
    stat    TEXT NOT NULL,
    value    INTEGER NOT NULL,
    effort  INTEGER,

    PRIMARY KEY (id, stat),

    CONSTRAINT pk_pokemon_base_stats_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""

create_table_sql_pokemon_description1 = """
DROP TABLE IF EXISTS pokemon_description1;
CREATE TABLE IF NOT EXISTS pokemon_description1 (
    id      SMALLSERIAL PRIMARY KEY,
    weight  SMALLSERIAL,
    height  SMALLSERIAL,
    species TEXT,
    sprite  TEXT,

    CONSTRAINT pk_pokemon_description1_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
)
"""

create_table_sql_pokemon_description2 = """
DROP TABLE IF EXISTS pokemon_description2;
CREATE TABLE IF NOT EXISTS pokemon_description2 (
    id        SMALLSERIAL PRIMARY KEY,
    genus     TEXT,
    shape     TEXT,
    habitat   TEXT,
    color     TEXT,
    evolves_from   TEXT,
    base_happiness SMALLSERIAL,
    hatch_counter  SMALLSERIAL,
    is_baby      BOOL,
    is_legendary BOOL,
    is_mythical  BOOL,
    capture_rate SMALLSERIAL,
    growth_rate  TEXT,
    gender_rate  SMALLSERIAL,
    has_gender_differences BOOL,

    CONSTRAINT pk_pokemon_description2_id
        FOREIGN KEY (id)
        REFERENCES pokemon_species(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
"""


db_cur.execute(create_table_sql_pokemon_species)
db_cur.execute(create_table_sql_pokemon_base_stats)
db_cur.execute(create_table_sql_pokemon_flavor_texts)
db_cur.execute(create_table_sql_pokemon_moves)
db_cur.execute(create_table_sql_pokemon_abilities)
db_cur.execute(create_table_sql_pokemon_types)
db_cur.execute(create_table_sql_pokemon_base_stats)
db_cur.execute(create_table_sql_pokemon_description1)
db_cur.execute(create_table_sql_pokemon_description2)
db_conn.commit()