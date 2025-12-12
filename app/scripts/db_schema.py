# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------
#PYSL
from pathlib import Path
import os
import logging.config

#PYPI
import psycopg2
from psycopg2 import sql
from yaml import safe_load
from dotenv import load_dotenv


load_dotenv()

# -------------------------------------------------------------------------
# SECTION: Set Up Logging
# -------------------------------------------------------------------------
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
fname = Path(__file__).name.replace(".py", "")
logger = logging.getLogger(fname)


# -------------------------------------------------------------------------
# SECTION: Creating Database/Tables
# -------------------------------------------------------------------------

def get_connection_to_db(db_name:str) -> psycopg2._psycopg.connection:
    """
    returns connection to database

    Parameters
    ----------
    db_name : str
        name of database to connect to

    Returns
    -------
    psycopg2._psycopg.connection
        returns connection to database
    """

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=db_name,
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")

    )
    return conn

def get_connection_to_postgres_db() -> psycopg2._psycopg.connection:
    """
    returns connection to 'postgres' database. Mostly for the purposes
    of creating and deleting databases.

    Returns
    -------
    psycopg2._psycopg.connection
        returns connection to database
    """

    conn = get_connection_to_db(db_name="postgres")
    return conn

def get_ddb_conn(db_name:str):
    """
    Creates a duckdb connection to interface with the database

    Parameters
    ----------
    db_name : str
        name of database to connect to

    Returns
    -------
    conn : duckdb.Connection
    """
    import duckdb as ddb
    pg_uri = (
        f"postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}"
        f"@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}"
        f"/{db_name}"
    )

    # 2️⃣  DuckDB: install/load extension, attach PG, run a query
    conn = ddb.connect()
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_uri}' AS pg (TYPE POSTGRES);")
    return conn

def is_connection_to_db_valid(db_name:str) -> bool:
    """
    Tests connection to database

    Parameters
    ----------
    db_name : str
        name of database to connect to

    Returns
    -------
    bool
        if connection is valid then True, else False
    """
    connection_valid = False
    try:
        conn = get_connection_to_db(db_name=db_name)
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


def create_db(db_name:str):
    """
    Create the target PostgreSQL database if it does not already exist.

    Parameters
    ----------
    db_name : str
        name of database to create

    Returns
    -------
    None
    """
    try:
        c = get_connection_to_postgres_db()
        c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        c.cursor().execute(sql.SQL("CREATE DATABASE {}")
                           .format(sql.Identifier(db_name)))
        c.close()
        logger.info(f"created database")
    except psycopg2.errors.DuplicateDatabase:
        logger.warning('Database already exists. Bypassing..')

def delete_db(db_name:str):
    """
    Delete the target PostgreSQL database if it does exist.

    Parameters
    ----------
    db_name : str
        name of database to drop

    Returns
    -------
    None
    """
    c = get_connection_to_postgres_db()
    c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    c.cursor().execute(sql.SQL("DROP DATABASE IF EXISTS {}")
                      .format(sql.Identifier(db_name)))
    c.close()
    logger.info(f"deleted database")

def create_tables(db_name:str) -> int:
    """
    Create all PostgreSQL tables required by the Pokémon ETL pipeline.

    Parameters
    ----------
    db_name : str
        name of database to create the tables in.

    Returns
    -------
    int
        1 if the tables were created (or already existed) and the
        transaction committed successfully; 0 if an exception occurred
        while executing any of the DDL statements.
    """
    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint0A
    # -------------------------------------------------------------------------
    create_table_sql_species = """
        CREATE TABLE IF NOT EXISTS species (
            species_id SMALLINT PRIMARY KEY,
            species_name TEXT,
            generation SMALLINT
        );
    """

    create_table_sql_type = """
        CREATE TABLE IF NOT EXISTS type (
            type_id SMALLINT PRIMARY KEY,
            type_name TEXT,
            generation SMALLINT
        );
    """

    create_table_sql_ability = """
        CREATE TABLE IF NOT EXISTS ability (
            ability_id SMALLINT PRIMARY KEY,
            ability_name TEXT,
            generation SMALLINT
        );
    """

    create_table_sql_move = """
        CREATE TABLE IF NOT EXISTS move (
            move_id SMALLINT PRIMARY KEY,
            move_name TEXT,
            generation SMALLINT
        );
    """

    create_table_sql_version_group = """
        CREATE TABLE IF NOT EXISTS version_group (
            version_group_id SMALLINT PRIMARY KEY,
            version_group_name TEXT,
            generation SMALLINT
        );
    """

    create_table_sql_main_region = """
        CREATE TABLE IF NOT EXISTS main_region (
            main_region_id SMALLINT PRIMARY KEY,
            main_region_name TEXT,
            generation SMALLINT
        );
    """

    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint0B
    # -------------------------------------------------------------------------
    create_table_sql_pokemon = """
        CREATE TABLE IF NOT EXISTS pokemon (
            pokemon_id SMALLINT PRIMARY KEY,
            pokemon_name TEXT
        );
    """


    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint1
    # -------------------------------------------------------------------------
    create_table_sql_pokemon_description = """
        CREATE TABLE IF NOT EXISTS pokemon_description (
            pokemon_id      SMALLINT PRIMARY KEY,
            weight  SMALLINT,
            height  SMALLINT,
            base_experience SMALLINT,
            species_id      SMALLINT,
            sprite  TEXT
        )
        """

    create_table_sql_pokemon_base_stats = """
    CREATE TABLE IF NOT EXISTS pokemon_base_stats (
        pokemon_id      SMALLINT NOT NULL,
        stat    TEXT NOT NULL,
        value    INTEGER NOT NULL,
        effort  INTEGER,

        PRIMARY KEY (pokemon_id, stat)
    );
    """

    create_table_sql_pokemon_moves = """
    CREATE TABLE IF NOT EXISTS pokemon_moves(
        pokemon_id   SMALLINT NOT NULL,
        move_id   SMALLINT,
        move      TEXT,
        level_learned_at   SMALLINT,
        learn_method  TEXT,
        sort_order    SMALLINT,
        version_group TEXT,

        PRIMARY KEY (pokemon_id, move_id, move, level_learned_at, learn_method, sort_order, version_group)
    );
    """

    create_table_sql_pokemon_abilities = """
    CREATE TABLE IF NOT EXISTS pokemon_abilities (
        pokemon_id SMALLINT NOT NULL,
        ability_id SMALLINT NOT NULL,
        ability    TEXT,
        slot       SMALLINT NOT NULL,
        is_hidden  BOOL,

        PRIMARY KEY (pokemon_id, ability_id)
    );
    """

    create_table_sql_pokemon_types = """
    CREATE TABLE IF NOT EXISTS pokemon_types (
        pokemon_id SMALLINT NOT NULL,
        type_id SMALLINT NOT NULL,  
        type    TEXT NOT NULL,
        slot    SMALLINT NOT NULL,

        PRIMARY KEY (pokemon_id, type_id)
    );
    """



    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint2
    # -------------------------------------------------------------------------
    create_table_sql_species_description = """
    CREATE TABLE IF NOT EXISTS species_description (
        species_id        SMALLINT PRIMARY KEY,
        genus     TEXT,
        shape     TEXT,
        habitat   TEXT,
        color     TEXT,
        evolves_from   TEXT,
        base_happiness SMALLINT,
        hatch_counter  SMALLINT,
        is_baby      BOOL,
        is_legendary BOOL,
        is_mythical  BOOL,
        capture_rate SMALLINT,
        growth_rate  TEXT,
        gender_rate  SMALLINT,
        has_gender_differences BOOL
    );
    """

    create_table_sql_species_varieties = """
        CREATE TABLE IF NOT EXISTS species_varieties (
            species_id SMALLINT,
            species_name TEXT,
            is_default BOOLEAN,
            pokemon_id SMALLINT,
            pokemon_name TEXT,

            PRIMARY KEY (species_id, pokemon_id)
        );
    """

    create_table_sql_species_flavor_texts = """
    CREATE TABLE IF NOT EXISTS species_flavor_texts(
        species_id   SMALLINT NOT NULL,
        text      TEXT,
        version   TEXT,

        PRIMARY KEY (species_id, version)
    );
    """


    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint3
    # -------------------------------------------------------------------------
    create_table_sql_move_attributes = """
    CREATE TABLE IF NOT EXISTS move_attributes (
        move_id SMALLINT NOT NULL PRIMARY KEY,
        name TEXT,
        type TEXT,
        damage_class TEXT,
        target TEXT,
        accuracy SMALLINT,
        power SMALLINT,
        pp SMALLINT,
        category TEXT,
        ailment TEXT,
        ailment_chance SMALLINT,
        crit_rate SMALLINT,
        drain SMALLINT,
        flinch_chance SMALLINT,
        healing SMALLINT,
        stat_chance SMALLINT,
        priority SMALLINT,
        effect_chance SMALLINT,
        effect_entry_short TEXT,
        effect_entry_long TEXT

    );
    """


    # -------------------------------------------------------------------------
    # SECTION: Used by apiendpoint4
    # -------------------------------------------------------------------------
    create_table_sql_type_damage_relations = """
    CREATE TABLE IF NOT EXISTS type_damage_relations (
        type_id SMALLINT NOT NULL,
        type_name TEXT, 
        rel_type_id SMALLINT NOT NULL, 
        rel_type_name TEXT, 
        damage_label TEXT NOT NULL,

        PRIMARY KEY (type_id, rel_type_id, damage_label)
    );
    """

    create_table_sql_type_sprites = """
    CREATE TABLE IF NOT EXISTS type_sprites (
        type_id SMALLINT NOT NULL PRIMARY KEY,
        type_name TEXT, 
        type_sprite TEXT
    );
    """

    # -------------------------------------------------------------------------
    # SECTION: Testing Connection
    # -------------------------------------------------------------------------
    connection_valid = is_connection_to_db_valid(db_name=db_name)


    # -------------------------------------------------------------------------
    # SECTION: Building Database Tables
    # -------------------------------------------------------------------------
    if connection_valid:
        try:
            db_conn = get_connection_to_db(db_name=db_name)
            db_cur = db_conn.cursor()

            db_cur.execute(create_table_sql_species)
            logger.debug("Success create_table_sql_specie")
            db_cur.execute(create_table_sql_type)
            logger.debug("Success create_table_sql_type")
            db_cur.execute(create_table_sql_ability)
            logger.debug("Success create_table_sql_ability")
            db_cur.execute(create_table_sql_move)
            logger.debug("Success create_table_sql_move")
            db_cur.execute(create_table_sql_version_group)
            logger.debug("Success create_table_sql_version_group")
            db_cur.execute(create_table_sql_main_region)
            logger.debug("Success create_table_sql_main_region")
            db_cur.execute(create_table_sql_pokemon)
            logger.debug("Success create_table_sql_pokemon")


            db_cur.execute(create_table_sql_pokemon_description)
            logger.debug("Success create_table_sql_pokemon_description")
            db_cur.execute(create_table_sql_pokemon_base_stats)
            logger.debug("Success create_table_sql_pokemon_base_stats")
            db_cur.execute(create_table_sql_pokemon_moves)
            logger.debug("Success create_table_sql_pokemon_moves")
            db_cur.execute(create_table_sql_pokemon_abilities)
            logger.debug("Success create_table_sql_pokemon_abilities")
            db_cur.execute(create_table_sql_pokemon_types)
            logger.debug("Success create_table_sql_pokemon_types")


            db_cur.execute(create_table_sql_species_description)
            logger.debug("Success create_table_sql_species_description")
            db_cur.execute(create_table_sql_species_flavor_texts)
            logger.debug("Success create_table_sql_species_flavor_texts")
            db_cur.execute(create_table_sql_species_varieties)
            logger.debug("Success create_table_sql_species_varieties")


            db_cur.execute(create_table_sql_move_attributes)
            logger.debug("Success create_table_sql_move_attributes")


            db_cur.execute(create_table_sql_type_damage_relations)
            logger.debug("Success create_table_sql_type_damage_relations")
            db_cur.execute(create_table_sql_type_sprites)
            logger.debug("Success create_table_sql_type_sprites")

            db_conn.commit()
            logger.info("Committed Successfully to pokemondb")
        except Exception as exc:
            logger.exception("Connection Error to pokemondb")
            return 0
    return 1

def add_constraints(db_name:str):
    """
    Add all foreign‑key constraints required by the Pokémon relational schema.
    Contraints added here due to dependencies of parent-child relationships
    not congruent to async nature of data extraction.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    conn = get_connection_to_db(db_name=db_name)
    conn.autocommit = True
    cur = conn.cursor()



    # ---------- pokemon_description > (1:1) pokemon ----------
    try:
        cur.execute("""
            ALTER TABLE pokemon_description
            ADD CONSTRAINT fk_pokemon
            FOREIGN KEY (pokemon_id)
            REFERENCES pokemon(pokemon_id)
            ON DELETE CASCADE;
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- pokemon_description > (m:1) species ----------
    try:
        cur.execute("""
            ALTER TABLE pokemon_description
            ADD CONSTRAINT fk_pd_species
            FOREIGN KEY (species_id)
            REFERENCES species(species_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- pokemon_abilities > (1:1) ability ----------
    try:
        cur.execute("""
            ALTER TABLE pokemon_abilities
            ADD CONSTRAINT fk_pa_ability
            FOREIGN KEY (ability_id)
            REFERENCES ability(ability_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- pokemon_types > (1:1) type ----------
    try:
        cur.execute("""
            ALTER TABLE pokemon_types
            ADD CONSTRAINT fk_pt_type
            FOREIGN KEY (type_id)
            REFERENCES type(type_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- species_description > (1:1) species ----------
    try:
        cur.execute("""
            ALTER TABLE species_description
            ADD CONSTRAINT fk_sd_species
            FOREIGN KEY (species_id)
            REFERENCES species(species_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- species_flavor_texts > (m:1) species ----------
    try:
        cur.execute("""
            ALTER TABLE species_flavor_texts
            ADD CONSTRAINT fk_sft_species
            FOREIGN KEY (species_id)
            REFERENCES species(species_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- species_varieties > (m:1) species ----------
    try:
        cur.execute("""
            ALTER TABLE species_varieties
            ADD CONSTRAINT fk_sv_species
            FOREIGN KEY (species_id)
            REFERENCES species(species_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- species_varieties > (m:1) pokemon ----------
    try:
        cur.execute("""
            ALTER TABLE species_varieties
            ADD CONSTRAINT fk_sv_pokemon
            FOREIGN KEY (pokemon_id)
            REFERENCES pokemon(pokemon_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- move_attributes > (1:1) move ----------
    try:
        cur.execute("""
            ALTER TABLE move_attributes
            ADD CONSTRAINT fk_ma_move
            FOREIGN KEY (move_id)
            REFERENCES move(move_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- type_damage_relations > (m:1) type ----------
    try:
        cur.execute("""
            ALTER TABLE type_damage_relations
            ADD CONSTRAINT fk_tdr_type
            FOREIGN KEY (type_id)
            REFERENCES type(type_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    # ---------- type_sprites > (m:1) type ----------
    try:
        cur.execute("""
            ALTER TABLE type_sprites
            ADD CONSTRAINT fk_ts_type
            FOREIGN KEY (type_id)
            REFERENCES type(type_id);
        """)
    except psycopg2.errors.DuplicateObject:
        # Constraint already exists – nothing to do.
        logger.info("Constraint fk_pokemon already exists. skipping.")

    cur.close()
    conn.close()
    print("All foreign‑key constraints added successfully.")



if __name__ == "__main__":
    db_name = "test"
    delete_db(db_name)
    create_db(db_name)
    create_tables(db_name)
    add_constraints(db_name)
    delete_db(db_name)