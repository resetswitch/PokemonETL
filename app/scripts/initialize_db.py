# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

import sys

import psycopg2
from psycopg2 import sql
import logging.config
import yaml
from pathlib import Path
import os
import sys

from dotenv import load_dotenv, dotenv_values # importing necessary functions from dotenv library
load_dotenv()

sys.path.append(os.getenv('PYTHON_SCRIPTS'))
import utility
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
# SECTION: Creating Database/Tables
# -------------------------------------------------------------------------

def create_db():
    try:
        c = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname="postgres",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")

        )
        c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c.cursor().execute(sql.SQL("CREATE DATABASE {}")
                           .format(sql.Identifier(os.getenv("POSTGRES_DB"))))
        c.close()
        logger.info(f"created database")
    except psycopg2.errors.DuplicateDatabase:
        logger.warning('Database already exists. Bypassing..')

def delete_db():


    c = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname="postgres",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")

    )
    c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    c.cursor().execute(sql.SQL("DROP DATABASE IF EXISTS {}")
                      .format(sql.Identifier(os.getenv("POSTGRES_DB"))))
    c.close()
    logger.info(f"deleted database")

def create_tables() -> int:
    create_table_sql_pokemon_species= """
    CREATE TABLE IF NOT EXISTS pokemon_species (
        poke_id SMALLSERIAL PRIMARY KEY,
        species TEXT,
        generation SMALLSERIAL
    );
    """

    create_table_sql_pokemon_base_stats = """
    CREATE TABLE IF NOT EXISTS pokemon_base_stats (
        poke_id      SMALLSERIAL NOT NULL,
        stat    TEXT NOT NULL,
        value    INTEGER NOT NULL,
        effort  INTEGER,
    
        PRIMARY KEY (poke_id, stat),
    
        CONSTRAINT pk_pokemon_base_stats_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    create_table_sql_pokemon_flavor_texts= """
    CREATE TABLE IF NOT EXISTS pokemon_flavor_texts(
        poke_id   SMALLSERIAL NOT NULL,
        text      TEXT,
        version   TEXT,
    
        PRIMARY KEY (poke_id, version),
    
        CONSTRAINT pk_pokemon_flavor_texts_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    create_table_sql_pokemon_moves= """
    CREATE TABLE IF NOT EXISTS pokemon_moves(
        poke_id   SMALLSERIAL NOT NULL,
        move      TEXT,
        level_learned_at   SMALLSERIAL,
        learn_method  TEXT,
        sort_order SMALLSERIAL,
        version_group TEXT,
    
        PRIMARY KEY (poke_id, move, level_learned_at, learn_method, sort_order, version_group),
    
        CONSTRAINT pk_pokemon_moves_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    create_table_sql_pokemon_abilities= """
    CREATE TABLE IF NOT EXISTS pokemon_abilities (
        poke_id   SMALLSERIAL NOT NULL,
        slot      SMALLSERIAL NOT NULL,
        ability   TEXT,
        is_hidden BOOL,
    
        PRIMARY KEY (poke_id, slot),
    
        CONSTRAINT pk_pokemon_abilities_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    create_table_sql_pokemon_types= """
    CREATE TABLE IF NOT EXISTS pokemon_types (
        poke_id SMALLSERIAL NOT NULL,
        slot    SMALLSERIAL NOT NULL,
        type    TEXT NOT NULL,
    
        PRIMARY KEY (poke_id, type),
    
        CONSTRAINT pk_pokemon_types_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    create_table_sql_pokemon_description1 = """
    CREATE TABLE IF NOT EXISTS pokemon_description1 (
        poke_id      SMALLSERIAL PRIMARY KEY,
        weight  SMALLSERIAL,
        height  SMALLSERIAL,
        species TEXT,
        sprite  TEXT,
    
        CONSTRAINT pk_pokemon_description1_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    )
    """

    create_table_sql_pokemon_description2 = """
    CREATE TABLE IF NOT EXISTS pokemon_description2 (
        poke_id        SMALLSERIAL PRIMARY KEY,
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
    
        CONSTRAINT pk_pokemon_description2_poke_id
            FOREIGN KEY (poke_id)
            REFERENCES pokemon_species(poke_id)
            ON UPDATE CASCADE
            ON DELETE RESTRICT
    );
    """

    # -------------------------------------------------------------------------
    # SECTION: Testing Connection
    # -------------------------------------------------------------------------
    connection_valid = utility.is_connection_to_db_valid()


    # -------------------------------------------------------------------------
    # SECTION: Building Database Tables
    # -------------------------------------------------------------------------
    if connection_valid:
        try:
            db_conn = utility.get_connection_to_db()
            db_cur = db_conn.cursor()

            db_cur.execute(create_table_sql_pokemon_species)
            logger.debug("Success create_table_sql_pokemon_species")
            db_cur.execute(create_table_sql_pokemon_base_stats)
            logger.debug("Success create_table_sql_pokemon_base_stats")
            db_cur.execute(create_table_sql_pokemon_flavor_texts)
            logger.debug("Success create_table_sql_pokemon_flavor_texts")
            db_cur.execute(create_table_sql_pokemon_moves)
            logger.debug("Success create_table_sql_pokemon_moves")
            db_cur.execute(create_table_sql_pokemon_abilities)
            logger.debug("Success create_table_sql_pokemon_abilities")
            db_cur.execute(create_table_sql_pokemon_types)
            logger.debug("Success create_table_sql_pokemon_types")
            db_cur.execute(create_table_sql_pokemon_description1)
            logger.debug("Success create_table_sql_pokemon_description1")
            db_cur.execute(create_table_sql_pokemon_description2)
            logger.debug("Success create_table_sql_pokemon_description2")
            db_conn.commit()
            logger.info("Committed Successfully to pokemondb")
        except Exception as exc:
            logger.exception("Connection Error to pokemondb")
            return 0
    return 1

if __name__ == "__main__":
    delete_db()
    create_db()
    create_tables()