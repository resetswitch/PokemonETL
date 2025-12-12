# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

# PSL
import os
from pathlib import Path
import logging
import logging.config

# PYPI
import requests
import psycopg2
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from psycopg2 import sql
from psycopg2.extras import execute_values
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import yaml
from dotenv import load_dotenv, dotenv_values # importing necessary functions from dotenv library
load_dotenv()

# -------------------------------------------------------------------------
# SECTION: Set Up Logging
# -------------------------------------------------------------------------
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = yaml.safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
logger = logging.getLogger("initialize_database")

# -------------------------------------------------------------------------
# SECTION: TENACITY RETRY SETTINGS
# -------------------------------------------------------------------------

RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.HTTPError,
)




# -------------------------------------------------------------------------
# SECTION: GENERAL UTILITY FUNCTIONS
# -------------------------------------------------------------------------
def make_connection() -> psycopg2.extensions.connection:
    """Create and return a new psycopg2 connection using environment variables.
    Returns:
        psycopg2.extensions.connection: An open connection ready for use.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),      # change if you connect to another DB
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    return conn


@retry(
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,          # re‑raise the last exception if we give up
)
def get_json(url: str, timeout: float = 10) -> dict:
    """
    Perform a GET request and return the parsed JSON payload.
    Raises on network errors, timeouts, or non‑2xx responses.

    Parameters
    ----------
    url : str
        URL for the new Request object
    timeout : float
        How many seconds to wait for the server to send data before giving up, as a float,

    Returns
    -------
    dict
        the json of the response object
    """
    response = requests.get(url, timeout=timeout)

    # Treat HTTP 5xx and 429 as retry‑able; everything else is final.
    if response.status_code in {429, 500, 502, 503, 504}:
        # Convert to an exception that Tenacity recognises
        raise requests.exceptions.HTTPError(
            f"Retryable status {response.status_code}", response=response
        )
    # Any other non‑2xx status is considered a hard failure
    response.raise_for_status()
    return response.json()

def get_url_segment(url: str, segment: int = -1) -> str:
    """
    extracts a desired segment from a url. -1 is the last, -2 is the second to last

    Parameters
    ----------
    url : str
        URL that contains segments
    segment: int
        is the desired segment to capture in the url. negative numbers are allowed.
        negative numbers reference the back segments first

    Returns
    -------
    str
        a string of the desired segment
    """
    if segment < 0 and url[-1] == '/':
        segment += -1
    return url.rsplit('/')[segment]



# -------------------------------------------------------------------------
# SECTION: API ETL FUNCTIONS
# -------------------------------------------------------------------------

def apiendpoint0_get_missing_generations() -> list[int]:
    """
    generates the missing generations based on values from the pokemon_species table.
    the last generation as of 2025-11-01 was generation 9.

    Returns
    -------
    list[int]
       a list of generations (eg. [1,5])
    """
    generations_missing = []
    try:
        url = f"https://pokeapi.co/api/v2/generation/"
        data = get_json(url)
        generations = [int(get_url_segment(gen['url'], -1)) for gen in data['results']]

        query = sql.SQL("""
            SELECT
                generation
            FROM pokemon_species as ps
            GROUP BY generation
        """
        )

        # Use a context manager so the connection / cursor close automatically.
        with make_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)  # params are bound safely
                # fetchall returns a list of tuples like [(val1,), (val2,), …]
                rows = cur.fetchall()

        # Flatten the list of 1‑element tuples into a plain list.
        generations_exist = sorted([row[0] for row in rows])

        generations_missing = [gen for gen in generations if int(gen) not in generations_exist]

    except Exception as exc:
        logger.error("All retries exhausted: %s", exc)

    return generations_missing

def apiendpoint0_transform(generation: int, data: dict) -> list[tuple]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/generation/{generation}/" api

    Parameters
    ----------
    generation: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    list[tuple]
        a list of tuples. Where each tuple is (id, species, generation)
    """
    records = []
    for d in data['pokemon_species']:
        species = d['species']
        id = int(d['url'].rsplit('/', 2)[-2])
        generation = generation
        record = (id, species, generation)
        records.append(record)
    return records

def apiendpoint0_to_db(generation: int, data: dict) -> int|None:
    """
    Transforms api data and uploads pokemon_species table in pokemon database.

    Parameters
    ----------
    generation: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful, None if no species data
    """
    species = apiendpoint0_transform(generation, data)
    if species is not None:

        upsert_sql_species = """
            INSERT INTO pokemon_species (id, species, generation)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                species = EXCLUDED.species,
                generation = EXCLUDED.generation;
        """

        try:
            with make_connection() as conn:
                with conn.cursor() as cur:
                    # execute_values expands the %s placeholder into a bulk VALUES list
                    execute_values(cur, upsert_sql_species, species, template=None, page_size=1000)
            logger.info(f"Success with Generation {generation}")
            return 1
        except psycopg2.DatabaseError as db_err:
            # Transaction rolled back automatically
            logger.error(f"Database error: {db_err}. Generation {generation}")
            return 0
            print(f"Database error: Generation {generation}")
        except Exception as exc:
            logger.error(f"Unexpected error: {exc}. Generation {generation}")
            return 0
            print(f"Unexpected error: Generation {generation}")
    return None

def apiendpoint0_transact() -> None:
    """
    ETL from pokeapi to Postgres for generalized specie information (id, specie, generation)
    where an example would look like (1, 'bulbasaur', 1)
    """
    generations = apiendpoint0_get_missing_generations()
    if len(generations) != 0:
        with Progress(
                "[progress.description]{task.description}",
                BarColumn(bar_width=None),  # auto‑size bar
                "[progress.percentage]{task.percentage:>3.0f}%",
                "•",
                TextColumn("[bold magenta]{task.completed}/{task.total}"),
                "•",
                TimeRemainingColumn(),
        ) as prog:
            task = prog.add_task("Rqst Gens", total=len(generations))

            for generation in generations:

                try:
                    url = f"https://pokeapi.co/api/v2/generation/{generation}/"
                    data = get_json(url)
                    apiendpoint0_to_db(generation, data)
                except Exception as exc:
                    logger.error("All retries exhausted: %s", exc)





def apiendpoint1_get_missing_ids() -> list[int]:
    """
    generates the missing ids based on values from the pokemon_species vs
    the pokemon_description1 table. This descerns which ids to call from
    the "https://pokeapi.co/api/v2/pokemon/{id}/" api

    Returns
    -------
    list[int]
       a list of pokemon species ids (eg. [1,151])
    """
    query = sql.SQL("""
        SELECT
            ps.id
        FROM pokemon_species as ps
        WHERE 1=1
            AND (ps.id NOT IN (
            SELECT
                pd1.id
            FROM pokemon_description1 AS pd1
        ))
    """
    )

    # Use a context manager so the connection / cursor close automatically.
    with make_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)          # params are bound safely
            # fetchall returns a list of tuples like [(val1,), (val2,), …]
            rows = cur.fetchall()

    # Flatten the list of 1‑element tuples into a plain list.
    return sorted([row[0] for row in rows])

def apiendpoint1_transform(id, data: dict) -> tuple:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon/{id}/" api

    Parameters
    ----------
    id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    tuple
        where a tuple is described as (description1, base_stats, types, abilities, moves).
        each item in the tuple is a list of varying length (eg. len(description1) != len(base_stats))
    """
    weight = data['weight']
    height = data['height']
    species = data['species']['url'].split('/')[-2]
    sprite = data['sprites']['front_default']

    description1 = [(id, weight, height, species, sprite)]

    base_stats = []
    for stat in data['stats']:
        s = (
            id,
            stat.get('stat', {}).get('name', None),
            stat.get('base_stat', None),
            stat.get('effort', None),
        )
        base_stats.append(s)

    types = []
    for _type in data['types']:
        t = (
            id,
            _type['slot'],
            _type.get('type', {}).get('name'),
        )
        types.append(t)

    abilities = []
    for ability in data['abilities']:
        a = (
            id,
            ability.get('slot', None),
            ability.get('ability', {}).get('name', None),
            ability.get('is_hidden', None),
        )
        abilities.append(a)

    moves = []
    for i, move in enumerate(data['moves']):
        m_name = move.get('move', {}).get('name')
        lgds = move.get('version_group_details', {})
        for lgd in lgds:
            order = lgd.get('order', None)
            if order is None:
                order = 0
            m = (
                id,
                m_name,
                lgd.get('level_learned_at'),
                lgd.get('move_learn_method').get('name'),
                order,
                lgd.get('version_group', {}).get('name', None),
            )
            moves.append(m)
    return (description1, base_stats, types, abilities, moves)

def apiendpoint1_to_db(id: int,data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon/{id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description1: (id, weight, height, species, sprite)
    - pokemon_base_stats: (id, stat, value, effort)
    - pokemon_types: (id, slot, type)
    - pokemon_abilities: (id, slot, ability, is_hidden)
    - pokemon_moves: (id, move, level_learned_at, learn_method, sort_order, version_group)

    Parameters
    ----------
    id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """
    description1, base_stats, types, abilities, moves = apiendpoint1_transform(id, data)

    upsert_sql_description1 = """
            INSERT INTO pokemon_description1 (id, weight, height, species, sprite)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                species = EXCLUDED.species,
                sprite = EXCLUDED.sprite;
            """

    upsert_sql_base_stats = """
            INSERT INTO pokemon_base_stats (id, stat, value, effort)
            VALUES %s
            ON CONFLICT (id, stat) DO UPDATE SET
                value = EXCLUDED.value,
                effort = EXCLUDED.effort;
            """

    upsert_sql_types = """
            INSERT INTO pokemon_types (id, slot, type)
            VALUES %s
            ON CONFLICT (id, type) DO UPDATE SET
                slot = EXCLUDED.slot,
                type = EXCLUDED.type;
            """

    upsert_sql_abilities = """
            INSERT INTO pokemon_abilities (id, slot, ability, is_hidden)
            VALUES %s
            ON CONFLICT (id, slot) DO UPDATE SET
                ability = EXCLUDED.ability,
                is_hidden = EXCLUDED.is_hidden;
            """

    upsert_sql_pokemon_moves = """
            INSERT INTO pokemon_moves (id, move, level_learned_at, learn_method, sort_order, version_group)
            VALUES %s
            ON CONFLICT (id, move, level_learned_at, learn_method, sort_order, version_group) DO NOTHING
            """

    try:
        with make_connection() as conn:
            with conn.cursor() as cur:
                # execute_values expands the %s placeholder into a bulk VALUES list
                execute_values(cur, upsert_sql_abilities, abilities, template=None, page_size=1000)
                execute_values(cur, upsert_sql_types, types, template=None, page_size=1000)
                execute_values(cur, upsert_sql_base_stats, base_stats, template=None, page_size=1000)
                execute_values(cur, upsert_sql_pokemon_moves, moves, template=None, page_size=1000)
                execute_values(cur, upsert_sql_description1, description1, template=None, page_size=1000)
                logger.info(f'Inserted Values Successfully: id:{id}')
        return 1
    except Exception as exc:
        logger.error(f"Issues upserting id:{id}.Error Message: {exc}")
        return 0

def apiendpoint1_transact() -> None:
    """
    ETL from "https://pokeapi.co/api/v2/pokemon/{id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description1: (id, weight, height, species, sprite)
    - pokemon_base_stats: (id, stat, value, effort)
    - pokemon_types: (id, slot, type)
    - pokemon_abilities: (id, slot, ability, is_hidden)
    - pokemon_moves: (id, move, level_learned_at, learn_method, sort_order, version_group)
    """
    ids = apiendpoint1_get_missing_ids()
    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),  # auto‑size bar
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    ) as prog:

        task = prog.add_task("Rqsting APIendpoint1", total=len(ids))

        for id in ids:

            try:
                url = f"https://pokeapi.co/api/v2/pokemon/{id}/"
                data = get_json(url)
                apiendpoint1_to_db(id, data)
            except Exception as exc:
                logger.error("All retries exhausted: %s", exc)

            prog.advance(task, advance=1)




def apiendpoint2_get_missing_ids() -> list[int]:
    """
    generates the missing ids based on values from the pokemon_species vs
    the pokemon_description1 table. This descerns which ids to call from
    the "https://pokeapi.co/api/v2/pokemon-species/{id}/" api

    Returns
    -------
    list[int]
       a list of pokemon species ids (eg. [1,151])
    """
    query = sql.SQL("""
        SELECT
            ps.id
        FROM pokemon_species as ps
        WHERE 1=1
            AND (ps.id NOT IN (
            SELECT
                pd2.id
            FROM pokemon_description2 AS pd2
        ))
    """
    )

    # Use a context manager so the connection / cursor close automatically.
    with make_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)          # params are bound safely
            # fetchall returns a list of tuples like [(val1,), (val2,), …]
            rows = cur.fetchall()

    # Flatten the list of 1‑element tuples into a plain list.
    return sorted([row[0] for row in rows])

def apiendpoint2_transform(id, data: dict) -> tuple:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon-species/{id}/" api

    Parameters
    ----------
    id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    tuple
        where a tuple is described as (description2, flavor_text_entries).
        each item in the tuple is a list of varying length (eg. len(description2) != len(flavor_text_entries))
    """
    base_happiness = data['base_happiness']
    capture_rate = data['capture_rate']
    color = data['color']['name']
    evolves_from = (inner := data.get("evolves_from_species")) and inner.get("name", None)
    genus_pre = [g.get('genus', None) for g in data['genera'] if g.get('language', {}).get('name', None) == 'en']
    genus = next(iter(genus_pre), None)
    gender_rate = data['gender_rate']
    growth_rate = data['growth_rate'].get('name', None)
    habitat = (inner := data.get('habitat')) and inner.get("name", None)
    has_gender_differences = data['has_gender_differences']
    hatch_counter = data['hatch_counter']
    is_baby = data['is_baby']
    is_legendary = data['is_legendary']
    is_mythical = data['is_mythical']
    shape = data['shape']['name']

    description2 = [(
        id,
        genus, shape, habitat, color, evolves_from,
        base_happiness, hatch_counter,
        is_baby, is_legendary, is_mythical,
        capture_rate, growth_rate, gender_rate,
        has_gender_differences
    )]

    flavor_text_entries = []
    for fte in data['flavor_text_entries']:
        if fte.get('language', {}).get('name', None) == 'en':
            flavor_txt_orig = fte.get('flavor_text', None)
            # flavor_txt = None
            # if flavor_text_entries:
            #     flavor_txt = re.sub('[\n\x0c]', '', flavor_txt_orig)
            f = (
                id,
                flavor_txt_orig,
                fte.get('version', {}).get('name', None),
            )
            flavor_text_entries.append(f)

    return (description2, flavor_text_entries)

def apiendpoint2_to_db(id: int,data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description2: (id, genus, shape, habitat, color,
        evolves_from, base_happiness, hatch_counter, is_baby,
        is_legendary, is_mythical, capture_rate, growth_rate,
        gender_rate, has_gender_differences)
    - pokemon_flavor_text_entries: (id, text, version)

    Parameters
    ----------
    id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """
    description2, flavor_text_entries = apiendpoint2_transform(id, data)

    upsert_sql_description2 = """
        INSERT INTO pokemon_description2 (id, genus, shape, habitat, color, evolves_from, base_happiness, hatch_counter, is_baby, is_legendary, is_mythical, capture_rate, growth_rate, gender_rate, has_gender_differences)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            genus = EXCLUDED.genus,
            shape = EXCLUDED.shape,
            habitat = EXCLUDED.habitat,
            color = EXCLUDED.color,
            evolves_from = EXCLUDED.evolves_from,
            base_happiness = EXCLUDED.base_happiness,
            hatch_counter = EXCLUDED.hatch_counter,
            is_baby = EXCLUDED.is_baby,
            is_legendary = EXCLUDED.is_legendary,
            is_mythical = EXCLUDED.is_mythical,
            capture_rate = EXCLUDED.capture_rate,
            growth_rate = EXCLUDED.growth_rate,
            gender_rate = EXCLUDED.gender_rate,
            has_gender_differences = EXCLUDED.has_gender_differences;
        """

    upsert_sql_flavor_text_entries = """
        INSERT INTO pokemon_flavor_texts (id, text, version)
        VALUES %s
        ON CONFLICT (id, version) DO UPDATE SET
            text = EXCLUDED.text,
            version = EXCLUDED.version;
        """
    try:
        with make_connection() as conn:
            with conn.cursor() as cur:
                # execute_values expands the %s placeholder into a bulk VALUES list
                execute_values(cur, upsert_sql_flavor_text_entries, flavor_text_entries, template=None, page_size=1000)
                execute_values(cur, upsert_sql_description2, description2, template=None, page_size=1000)
        return 1
    except Exception as exc:
        logger.error(f"Issues upserting id:{id}.Error Message: {exc}")
        return 0

def apiendpoint2_transact() -> None:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description2: (id, genus, shape, habitat, color,
        evolves_from, base_happiness, hatch_counter, is_baby,
        is_legendary, is_mythical, capture_rate, growth_rate,
        gender_rate, has_gender_differences)
    - pokemon_flavor_text_entries: (id, text, version)
    """
    ids = apiendpoint2_get_missing_ids()
    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),  # auto‑size bar
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    ) as prog:

        task = prog.add_task("Rqsting APIendpoint2", total=len(ids))

        for id in ids:

            try:
                url = f"https://pokeapi.co/api/v2/pokemon-species/{id}/"
                data = get_json(url)
                apiendpoint2_to_db(id, data)
            except Exception as exc:
                logger.error("All retries exhausted: %s", exc)

            prog.advance(task, advance=1)



if __name__ == "__main__":

    apiendpoint0_transact()
    apiendpoint1_transact()
    apiendpoint2_transact()
