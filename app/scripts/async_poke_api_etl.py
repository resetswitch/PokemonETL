# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

# PSL
import os
from pathlib import Path
import logging.config

# PYPI
import requests
import aiohttp
import asyncio
import asyncpg
from contextlib import asynccontextmanager
import psycopg2
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from psycopg2 import sql
from psycopg2.extras import execute_values
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log
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
fname = Path(__file__).name.replace(".py", "")
logger = logging.getLogger(fname)

# -------------------------------------------------------------------------
# SECTION: TENACITY RETRY SETTINGS
# -------------------------------------------------------------------------

RETRYABLE_EXCEPTIONS = (
    # requests.exceptions.ConnectionError,
    # requests.exceptions.Timeout,
    # requests.exceptions.HTTPError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    aiohttp.ServerTimeoutError,
    aiohttp.ClientResponseError,
)


# -------------------------------------------------------------------------
# SECTION: DECLARING MUDULE-LEVEL POOL
# -------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


# -------------------------------------------------------------------------
# SECTION: GENERAL UTILITY FUNCTIONS
# -------------------------------------------------------------------------


async def get_json(url: str, timeout: float = 10) -> dict:
    """Get JSON with exponential back‑off and Tenacity async retry."""
    async for attempt in AsyncRetrying(
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    ):
        with attempt:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.get(url) as resp:
                    # Treat 5xx / 429 as retry‑able
                    if resp.status in {429, 500, 502, 503, 504}:
                        raise aiohttp.ClientResponseError(
                            request_info=resp.request_info,
                            history=resp.history,
                            status=resp.status,
                            message=f"Retryable status {resp.status}",
                            headers=resp.headers,
                        )
                    resp.raise_for_status()
                    return await resp.json()

async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        logger.debug("Creating asyncpg connection pool …")
        _pool = await asyncpg.create_pool(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT")),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            min_size=1,
            max_size=10,          # tune to your workload
        )
    return _pool

@asynccontextmanager
async def get_connection() -> asyncpg.Connection:
    pool = await init_pool()
    # return await pool.acquire()
    async with pool.acquire() as conn:  # <-- pool.acquire() is itself a context manager
        try:
            yield conn
        finally:
            # The `async with pool.acquire()` block already releases the connection,
            # so nothing extra is required here. The `finally` is kept for symmetry
            # and future extensions (e.g., logging).
            pass


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

async def apiendpoint0_get_missing_generations() -> list[int]:
    """
    generates the missing generations based on values from the pokemon_species table.
    the last generation as of 2025-11-01 was generation 9.

    Returns
    -------
    list[int]
       a list of generations (eg. [1,5])
    """
    generations_missing = []

    url = f"https://pokeapi.co/api/v2/generation/"
    data = await get_json(url)
    generations = [int(get_url_segment(gen['url'], -1)) for gen in data['results']]

    query = """
        SELECT
            generation
        FROM pokemon_species as ps
        GROUP BY generation
    """


        # Use a context manager so the connection / cursor close automatically.
    async with get_connection() as conn:
        # try:
        rows = await conn.fetch(query)
        # except Exception as exc:
        #     logger.exception("Failed Getting Generations", exc_info=True)

        # Flatten the list of 1‑element tuples into a plain list.
        generations_exist = sorted([row[0] for row in rows])
        generations_missing = [gen for gen in generations if int(gen) not in generations_exist]

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
        a list of tuples. Where each tuple is (poke_id, species, generation)
    """
    records = []
    for d in data['pokemon_species']:
        species = d['name']
        poke_id = int(d['url'].rsplit('/', 2)[-2])
        generation = generation
        record = (poke_id, species, generation)
        records.append(record)
    return records

async def apiendpoint0_to_db(generation: int, data: dict) -> int|None:
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

    if not species:
        return None

    upsert_sql_species = """
        INSERT INTO pokemon_species (poke_id, species, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (poke_id) DO UPDATE SET
            species = EXCLUDED.species,
            generation = EXCLUDED.generation;
    """


    async with get_connection() as conn:
        try:
            await conn.executemany(upsert_sql_species, species)
            logger.info(f"Success with Generation {generation}")
            return 1
        except psycopg2.DatabaseError as db_err:
            logger.error(f"Database error: {db_err}. Generation {generation}")
            return 0
        except Exception as exc:
            logger.exception(f"Unexpected error: {exc}. Generation {generation}", exc_info=True)
            return 0

async def apiendpoint0_transact(generations:list[int]) -> int|None:
    """
    ETL from pokeapi to Postgres for generalized specie information (poke_id, specie, generation)
    where an example would look like (1, 'bulbasaur', 1)

    Parameters
    ----------
    generations: list[int]
       a list of generations (eg. [1,5])
    """

    if not generations:
        return None

    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),  # auto‑size bar
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    ) as prog:
        task = prog.add_task("Fetching Generations", total=len(generations))

        for gen in generations:

            try:
                url = f"https://pokeapi.co/api/v2/generation/{gen}/"
                data = await get_json(url)
                await apiendpoint0_to_db(gen, data)
            except Exception as exc:
                logger.exception(f"Generation {gen} failed", exc_info=True)
            prog.advance(task, advance=1)

        return 1





async def apiendpoint1_get_missing_poke_ids() -> list[int]:
    """
    generates the missing poke_ids based on values from the pokemon_species vs
    the pokemon_description1 table. This descerns which poke_ids to call from
    the "https://pokeapi.co/api/v2/pokemon/{poke_id}/" api

    Returns
    -------
    list[int]
       a list of pokemon species poke_ids (eg. [1,151])
    """
    query = """
        SELECT
            ps.poke_id
        FROM pokemon_species as ps
        WHERE 1=1
            AND (ps.poke_id NOT IN (
            SELECT
                pd1.poke_id
            FROM pokemon_description1 AS pd1
        ))
    """


    async with get_connection() as conn:
        rows = await conn.fetch(query)

        return sorted([row[0] for row in rows])

def apiendpoint1_transform(poke_id, data: dict) -> tuple:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon/{poke_id}/" api

    Parameters
    ----------
    poke_id: int
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

    description1 = [(poke_id, weight, height, species, sprite)]

    base_stats = []
    for stat in data['stats']:
        s = (
            poke_id,
            stat.get('stat', {}).get('name', None),
            stat.get('base_stat', None),
            stat.get('effort', None),
        )
        base_stats.append(s)

    types = []
    for _type in data['types']:
        t = (
            poke_id,
            _type['slot'],
            _type.get('type', {}).get('name'),
        )
        types.append(t)

    abilities = []
    for ability in data['abilities']:
        a = (
            poke_id,
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
                poke_id,
                m_name,
                lgd.get('level_learned_at'),
                lgd.get('move_learn_method').get('name'),
                order,
                lgd.get('version_group', {}).get('name', None),
            )
            moves.append(m)
    return (description1, base_stats, types, abilities, moves)

async def apiendpoint1_to_db(poke_id: int,data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon/{poke_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description1: (poke_id, weight, height, species, sprite)
    - pokemon_base_stats: (poke_id, stat, value, effort)
    - pokemon_types: (poke_id, slot, type)
    - pokemon_abilities: (poke_id, slot, ability, is_hidden)
    - pokemon_moves: (poke_id, move, level_learned_at, learn_method, sort_order, version_group)

    Parameters
    ----------
    poke_id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """



    description1, base_stats, types, abilities, moves = apiendpoint1_transform(poke_id, data)

    upsert_sql_description1 = """
            INSERT INTO pokemon_description1 (poke_id, weight, height, species, sprite)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (poke_id) DO UPDATE SET
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                species = EXCLUDED.species,
                sprite = EXCLUDED.sprite;
            """

    upsert_sql_base_stats = """
            INSERT INTO pokemon_base_stats (poke_id, stat, value, effort)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (poke_id, stat) DO UPDATE SET
                value = EXCLUDED.value,
                effort = EXCLUDED.effort;
            """

    upsert_sql_types = """
            INSERT INTO pokemon_types (poke_id, slot, type)
            VALUES ($1,$2,$3)
            ON CONFLICT (poke_id, type) DO UPDATE SET
                slot = EXCLUDED.slot,
                type = EXCLUDED.type;
            """

    upsert_sql_abilities = """
            INSERT INTO pokemon_abilities (poke_id, slot, ability, is_hidden)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (poke_id, slot) DO UPDATE SET
                ability = EXCLUDED.ability,
                is_hidden = EXCLUDED.is_hidden;
            """

    upsert_sql_pokemon_moves = """
            INSERT INTO pokemon_moves (poke_id, move, level_learned_at, learn_method, sort_order, version_group)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (poke_id, move, level_learned_at, learn_method, sort_order, version_group) DO NOTHING
            """
    async with get_connection() as conn:
        try:
            await conn.executemany(upsert_sql_description1, description1)
            await conn.executemany(upsert_sql_base_stats, base_stats)
            await conn.executemany(upsert_sql_types, types)
            await conn.executemany(upsert_sql_abilities, abilities)
            await conn.executemany(upsert_sql_pokemon_moves, moves)
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting poke_id:{poke_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint1_transact(poke_ids: list[int]) -> None:
    """
    ETL from "https://pokeapi.co/api/v2/pokemon/{poke_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description1: (poke_id, weight, height, species, sprite)
    - pokemon_base_stats: (poke_id, stat, value, effort)
    - pokemon_types: (poke_id, slot, type)
    - pokemon_abilities: (poke_id, slot, ability, is_hidden)
    - pokemon_moves: (poke_id, move, level_learned_at, learn_method, sort_order, version_group)

    Parameters
    ----------
    poke_ids: list[int]
       a list of Pokémon species poke_ids (eg. [1,151])
    """

    if not poke_ids:
        return None

    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    ) as prog:

        task = prog.add_task("Fetching Pokémon details", total=len(poke_ids))

        for pid in poke_ids:
            try:
                url = f"https://pokeapi.co/api/v2/pokemon/{pid}/"
                data = await get_json(url)
                await apiendpoint1_to_db(pid, data)
            except Exception as exc:
                logger.exception(f"poke_id {pid} failed", exc_info=True)
            prog.advance(task, advance=1)




async def apiendpoint2_get_missing_poke_ids() -> list[int]:
    """
    generates the missing poke_ids based on values from the pokemon_species vs
    the pokemon_description1 table. This descerns which poke_ids to call from
    the "https://pokeapi.co/api/v2/pokemon-species/{poke_id}/" api

    Returns
    -------
    list[int]
       a list of pokemon species poke_ids (eg. [1,151])
    """
    query = """
        SELECT
            ps.poke_id
        FROM pokemon_species as ps
        WHERE 1=1
            AND (ps.poke_id NOT IN (
            SELECT
                pd2.poke_id
            FROM pokemon_description2 AS pd2
        ))
    """

    async with get_connection() as conn:
        rows = await conn.fetch(query)

        # Flatten the list of 1‑element tuples into a plain list.
        return sorted([row[0] for row in rows])

def apiendpoint2_transform(poke_id, data: dict) -> tuple:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon-species/{poke_id}/" api

    Parameters
    ----------
    poke_id: int
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
        poke_id,
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
                poke_id,
                flavor_txt_orig,
                fte.get('version', {}).get('name', None),
            )
            flavor_text_entries.append(f)

    return (description2, flavor_text_entries)

async def apiendpoint2_to_db(poke_id: int,data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{poke_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description2: (poke_id, genus, shape, habitat, color,
        evolves_from, base_happiness, hatch_counter, is_baby,
        is_legendary, is_mythical, capture_rate, growth_rate,
        gender_rate, has_gender_differences)
    - pokemon_flavor_text_entries: (poke_id, text, version)

    Parameters
    ----------
    poke_id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """
    description2, flavor_text_entries = apiendpoint2_transform(poke_id, data)

    upsert_sql_description2 = """
        INSERT INTO pokemon_description2 (poke_id, genus, shape, habitat, color, evolves_from, base_happiness, hatch_counter, is_baby, is_legendary, is_mythical, capture_rate, growth_rate, gender_rate, has_gender_differences)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (poke_id) DO UPDATE SET
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
        INSERT INTO pokemon_flavor_texts (poke_id, text, version)
        VALUES ($1,$2,$3)
        ON CONFLICT (poke_id, version) DO UPDATE SET
            text = EXCLUDED.text,
            version = EXCLUDED.version;
        """

    async with get_connection() as conn:
        try:
            await conn.executemany(upsert_sql_description2, description2)
            await conn.executemany(upsert_sql_flavor_text_entries, flavor_text_entries)
            logger.info(f'Inserted Values Successfully: poke_id:{poke_id}')
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting poke_id:{poke_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint2_transact(poke_ids: list[int]) -> None:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{poke_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description2: (poke_id, genus, shape, habitat, color,
        evolves_from, base_happiness, hatch_counter, is_baby,
        is_legendary, is_mythical, capture_rate, growth_rate,
        gender_rate, has_gender_differences)
    - pokemon_flavor_text_entries: (poke_id, text, version)

    Parameters
    ----------
    poke_ids: list[int]
       a list of pokemon species poke_ids (eg. [1,151])
    """
    if not poke_ids:
      return None

    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),  # auto‑size bar
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    ) as prog:

        task = prog.add_task("Fetching species details", total=len(poke_ids))

        for pid in poke_ids:
            try:
                url = f"https://pokeapi.co/api/v2/pokemon-species/{pid}/"
                data = await get_json(url)
                await apiendpoint2_to_db(pid, data)
            except Exception as exc:
                logger.exception(f"Species {pid} failed", exc_info=True)
            prog.advance(task, advance=1)



# -------------------------------------------------------
# Helper to fetch the IDs that each downstream stage needs.
# These helpers already exist in the async version of the script.
# ----------------------------------------------------------------------
# async def apiendpoint1_get_missing_poke_ids() -> list[int]: ...
# async def apiendpoint2_get_missing_poke_ids() -> list[int]: ...

async def main() -> None:
    """
    Orchestrates the three ETL stages respecting the dependency chain:
    0 → (1 & 2).
    """
    # --------------------------------------------
    #    Run the generation stage first – it populates the
    #    `pokemon_species` table that the later stages query.
    # --------------------------------------------------------------
    generations = await apiendpoint0_get_missing_generations()
    await apiendpoint0_transact(generations)   # ← blocks until all generations are stored

    # --------------------------------------------------------------
    #   After stage‑0 is guaranteed to be complete, we can safely
    #    ask the DB for the IDs each downstream job still needs.
    # --------------------------------------------------------------
    poke_ids_ep1 = await apiendpoint1_get_missing_poke_ids()
    poke_ids_ep2 = await apiendpoint2_get_missing_poke_ids()

    # --------------------------------------------------------------
    #   Launch the two independent stages **concurrently**.
    #    `asyncio.gather` runs both coroutines in parallel and
    #    returns when *both* have finished.
    # --------------------------------------------------------------
    await asyncio.gather(
        apiendpoint1_transact(poke_ids_ep1),
        apiendpoint2_transact(poke_ids_ep2),
    )

    # --------------------------------------------------------------
    #   Clean‑up: close the asyncpg connection pool.
    # --------------------------------------------------------------
    if _pool:                     # `_pool` is the module‑level asyncpg pool
        await _pool.close()


if __name__ == "__main__":
    print('hello')
    # `asyncio.run` creates the event loop, runs `main`, and shuts the loop down.
    asyncio.run(main())
