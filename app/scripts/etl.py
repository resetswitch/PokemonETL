# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

# PSL
import os
from pathlib import Path
import logging.config
from typing import Any

# PYPI
import asyncio
import psycopg2
import yaml
from dotenv import load_dotenv

# Project
from utility import (
    close_pool,
    get_connection,
    get_json,
    get_url_segment,
    create_progress_bar,
    Progress
)
from db_schema import (
    create_db,
    delete_db,
    create_tables
)
load_dotenv()

# -------------------------------------------------------------------------
# SECTION: Set Up Logging
# -------------------------------------------------------------------------
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = yaml.safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
fname = Path(__file__).name.replace(".py", "")
logger = logging.getLogger(fname)  # fname is the name in the logger_config.yaml (loggers -> [name])


# -------------------------------------------------------------------------
# SECTION: API ETL FUNCTIONS
# -------------------------------------------------------------------------

async def apiendpoint0A_get_missing_generations(db_name: str) -> list[int]:
    """
    generates the missing generations based on values from the species table.
    the last generation as of 2025-11-01 was generation 9.

    Returns
    -------
    db_name : str
        name of database to load data into
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
        FROM species as ps
        GROUP BY generation
    """

    # Use a context manager so the connection / cursor close automatically.
    async with get_connection(db_name=db_name) as conn:
        # try:
        rows = await conn.fetch(query)
        # except Exception as exc:
        #     logger.exception("Failed Getting Generations", exc_info=True)

        # Flatten the list of 1‑element tuples into a plain list.
        generations_exist = sorted([row[0] for row in rows])
        generations_missing = [gen for gen in generations if int(gen) not in generations_exist]

        return generations_missing

def apiendpoint0A_transform(data: dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/generation/{generation}/" api

    Parameters
    ----------
    data: dict
        the json data generated from the api

    Returns
    -------
    dictionary of lists of tuples
        example of the items:
        {
            species: [(1, 'name', 1)..]
            types: [(1, 'name', 1)..]
            abilities: [(1, 'name', 1)..]
            moves: [(1, 'name', 1)..]
            version_groups: [(1, 'name', 1)..]
            main_regions: [(1, 'name', 1)..]
        }
    """

    generation = int(data['id'])

    species_records = []
    for d in data['pokemon_species']:
        species_name = d['name']
        species_id = int(d['url'].rsplit('/', 2)[-2])
        species_record = (species_id, species_name, generation)
        species_records.append(species_record)

    type_records = []
    for d in data['types']:
        type_name = d['name']
        type_id = int(d['url'].rsplit('/', 2)[-2])
        types_record = (type_id, type_name, generation)
        type_records.append(types_record)

    ability_records = []
    for d in data['abilities']:
        ability_name = d['name']
        ability_id = int(d['url'].rsplit('/', 2)[-2])
        ability_record = (ability_id, ability_name, generation)
        ability_records.append(ability_record)

    move_records = []
    for d in data['moves']:
        move_name = d['name']
        move_id = int(d['url'].rsplit('/', 2)[-2])
        move_record = (move_id, move_name, generation)
        move_records.append(move_record)

    version_group_records = []
    for d in data['version_groups']:
        vg_name = d['name']
        vg_id = int(d['url'].rsplit('/', 2)[-2])
        vg_record = (vg_id, vg_name, generation)
        version_group_records.append(vg_record)

    main_region_dict = data['main_region']
    main_region_name = main_region_dict['name']
    main_region_id = int(main_region_dict['url'].rsplit('/', 2)[-2])
    main_region_records = [(main_region_id, main_region_name, generation)]

    return {
        'species': species_records,
        'type': type_records,
        'ability': ability_records,
        'move': move_records,
        'version_group': version_group_records,
        'main_region': main_region_records
    }

async def apiendpoint0A_to_db(db_name:str, generation: int, data: dict) -> int:
    """
    Transforms api data and uploads to the core tables in pokemondb.

    Parameters
    ----------
    db_name : str
        name of database to load data into
    generation : int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful
    """
    transformed_data = apiendpoint0A_transform(data)

    upsert_sql_species = """
        INSERT INTO species (species_id, species_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (species_id) DO UPDATE SET
            species_name = EXCLUDED.species_name,
            generation = EXCLUDED.generation;
    """

    upsert_sql_type = """
        INSERT INTO type (type_id, type_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (type_id) DO UPDATE SET
            type_name = EXCLUDED.type_name,
            generation = EXCLUDED.generation;
    """

    upsert_sql_ability = """
        INSERT INTO ability (ability_id, ability_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (ability_id) DO UPDATE SET
            ability_name = EXCLUDED.ability_name,
            generation = EXCLUDED.generation;
    """

    upsert_sql_move = """
        INSERT INTO move (move_id, move_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (move_id) DO UPDATE SET
            move_name = EXCLUDED.move_name,
            generation = EXCLUDED.generation;
    """

    upsert_sql_version_group = """
        INSERT INTO version_group (version_group_id, version_group_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (version_group_id) DO UPDATE SET
            version_group_name = EXCLUDED.version_group_name,
            generation = EXCLUDED.generation;
    """

    upsert_sql_main_region = """
        INSERT INTO main_region (main_region_id, main_region_name, generation)
        VALUES ($1, $2, $3)
        ON CONFLICT (main_region_id) DO UPDATE SET
            main_region_name = EXCLUDED.main_region_name,
            generation = EXCLUDED.generation;
    """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_species, transformed_data['species'])
            logger.info(f"Success with generation {generation} - 'species'")
            await conn.executemany(upsert_sql_type, transformed_data['type'])
            logger.info(f"Success with generation {generation} - 'type'")
            await conn.executemany(upsert_sql_ability, transformed_data['ability'])
            logger.info(f"Success with generation {generation} - 'ability'")
            await conn.executemany(upsert_sql_move, transformed_data['move'])
            logger.info(f"Success with generation {generation} - 'sql_move'")
            await conn.executemany(upsert_sql_version_group, transformed_data['version_group'])
            logger.info(f"Success with generation {generation} - 'version_group'")
            await conn.executemany(upsert_sql_main_region, transformed_data['main_region'])
            logger.info(f"Success with generation {generation} - 'main_region'")
            return 1
        except psycopg2.DatabaseError as db_err:
            logger.error(f"Database error: {db_err}. Generation {generation}")
            return 0
        except Exception as exc:
            logger.exception(f"Unexpected error: {exc}. Generation {generation}", exc_info=True)
            return 0

async def apiendpoint0A_transact(db_name:str, generations: list[int], prog: Progress, task_id:int) -> int | None:
    """
    ETL from 'https://pokeapi.co/api/v2/generation/{gen}/' to pokemondb covering tables:
    species, type, ability, move, version_group, main_region

    Parameters
    ----------
    db_name : str
        name of database to load data into
    generations : list[int]
       a list of generations (eg. [1,5])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """

    if not generations:
        return None

    for gen in generations:

        try:
            url = f"https://pokeapi.co/api/v2/generation/{gen}/"
            data = await get_json(url)
            await apiendpoint0A_to_db(db_name=db_name, generation=gen, data=data)
        except Exception as exc:
            logger.exception(f"Generation {gen} failed", exc_info=True)
        prog.advance(task_id, advance=1)

    return 1



async def apiendpoint0B_urls() -> list[str]:
    """
    creates the urls to query

    Returns
    -------
    list[int]
       a list of urls (eg. [
       "https://pokeapi.co/api/v2/pokemon?offset=0&limit=20",
       "https://pokeapi.co/api/v2/pokemon?offset=20&limit=20"
       ])
    """

    def ceiling_division(numerator, denominator):
        return -(-numerator // denominator)

    url_initial = f"https://pokeapi.co/api/v2/pokemon"
    data_initial = await get_json(url_initial)

    limit = 20 # Cannot be higher due to api limitations
    iterations = ceiling_division(data_initial['count'],limit)
    offset = 0

    urls = []
    for i in range(iterations):
        url = f"https://pokeapi.co/api/v2/pokemon?offset={offset}&limit={limit}"
        urls.append(url)
        offset += limit
    return urls

def apiendpoint0B_transform(data:dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon/" api

    Parameters
    ----------
    data: dict
        the json data generated from the api

    Returns
    -------
    dictionary of lists of tuples
        example of the items:
        {
            pokemon: [(1, 'name')..]
        }
    """
    results = data.get('results')

    pid_records = []
    for r in results:
        pid = int(get_url_segment(r['url'], segment=-1))
        name = r['name']
        pid_records.append((pid,name))

    return {
        'pokemons': pid_records
    }

async def apiendpoint0B_to_db(db_name: str, data:dict) -> int:
    """
    Transforms api data and uploads to the pokemon table.

    Parameters
    ----------
    db_name : str
        name of database to load data into
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful
    """

    transformed_data = apiendpoint0B_transform(data)
    upsert_sql_pokemon = """
        INSERT INTO pokemon (pokemon_id, pokemon_name)
        VALUES ($1, $2)
        ON CONFLICT (pokemon_id) DO UPDATE SET
            pokemon_name = EXCLUDED.pokemon_name
    """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_pokemon, transformed_data['pokemons'])
            logger.info(f"Success with table pokemon")
            return 1
        except psycopg2.DatabaseError as db_err:
            logger.error(f"Database error: {db_err}. Bulk upsert to table pokemon")
            return 0
        except Exception as exc:
            logger.exception(f"Unexpected error: {exc}. Bulk upsert to table pokemon", exc_info=True)
            return 0

async def apiendpoint0B_transact(db_name: str, urls: list[str], prog: Progress, task_id:int):
    """
    ETL from 'f"https://pokeapi.co/api/v2/pokemon' to pokemondb covering the pokemon table

    Parameters
    ----------
    db_name : str
        name of database to load data into
    generations: list[int]
       a list of generations (eg. [1,5])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """
    for url in urls:
        try:
            data = await get_json(url)
            await apiendpoint0B_to_db(db_name=db_name, data=data)
        except Exception as exc:
            logger.exception(f"url {url} failed", exc_info=True)
        prog.advance(task_id, advance=1)



async def apiendpoint1_get_missing_pokemon(db_name: str) -> list[int]:
    """
    generates the missing pokemon ids based on values from the pokemon vs
    the pokemon_description table. This descerns which pokemon ids to call from
    the "https://pokeapi.co/api/v2/pokemon/{poke_id}/" api
    Parameters
    ----------
    db_name : str
        name of database to load data into

    Returns
    -------
    list[int]
       a list of pokemon species poke_ids (eg. [1,151])
    """

    query = """
        SELECT
            ps.pokemon_id
        FROM pokemon as ps
        WHERE 1=1
            AND (ps.pokemon_id NOT IN (
            SELECT
                pd.pokemon_id
            FROM pokemon_description AS pd
        ))
    """


    async with get_connection(db_name=db_name) as conn:
        rows = await conn.fetch(query)

        return sorted([row[0] for row in rows])

def apiendpoint1_transform(data: dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon/{pokemon_id}/" api

    Parameters
    ----------
    data: dict
        the json data generated from the api

    Returns
    -------
    dict
        {
            'description': [(),()..],
            'base_stats': [(),()..],
            'types': [(),()..],
            'abilities': [(),()..],
            'moves': [(),()..],
        }
    """
    pokemon_id = data['id']

    weight = data['weight']
    height = data['height']
    base_experience = data['base_experience']
    species_id = int(data['species']['url'].split('/')[-2])
    sprite = data['sprites']['front_default']

    description = [(pokemon_id, weight, height, base_experience, species_id, sprite)]

    base_stats = []
    for stat in data['stats']:
        s = (
            pokemon_id,
            stat.get('stat', {}).get('name', None),
            stat.get('base_stat', None),
            stat.get('effort', None),
        )
        base_stats.append(s)

    types = []
    for _type in data['types']:
        t = (
            pokemon_id,
            _type['slot'],
            _type.get('type', {}).get('name'),
            int(get_url_segment(_type.get('type', {}).get('url'), segment=-1)),
        )
        types.append(t)

    abilities = []
    for ability in data['abilities']:
        a = (
            pokemon_id,
            ability.get('slot', None),
            ability.get('ability', {}).get('name', None),
            int(get_url_segment(ability.get('ability', {}).get('url'), segment=-1)),
            ability.get('is_hidden', None),
        )
        abilities.append(a)

    moves = []
    for i, move in enumerate(data['moves']):
        m = move.get('move', {})
        m_name = m.get('name')
        m_id = int(get_url_segment(m.get('url'), segment=-1))
        lgds = move.get('version_group_details', {})
        for lgd in lgds:
            order = lgd.get('order', None)
            if order is None:
                order = 0
            m = (
                pokemon_id,
                m_name,
                m_id,
                lgd.get('level_learned_at'),
                lgd.get('move_learn_method').get('name'),
                order,
                lgd.get('version_group', {}).get('name', None),
            )
            moves.append(m)
    return {
        'description': description,
        'base_stats': base_stats,
        'types': types,
        'abilities': abilities,
        'moves': moves
    }

async def apiendpoint1_to_db(db_name: str, pokemon_id: int, data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon/{pokemon_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description: (pokemon_id, weight, height, base_experience, species_id, sprite)
    - pokemon_base_stats: (pokemon_id, stat, value, effort)
    - pokemon_types: (pokemon_id, slot, type)
    - pokemon_abilities: (pokemon_id, slot, ability, is_hidden)
    - pokemon_moves: (pokemon_id, move, level_learned_at, learn_method, sort_order, version_group)

    Parameters
    ----------
    db_name : str
        name of database to load data into
    pokemon_id: int
        the generation used in the api
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """

    transformed_data = apiendpoint1_transform(data)

    upsert_sql_pokemon_description = """
            INSERT INTO pokemon_description (pokemon_id, weight, height, base_experience, species_id, sprite)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (pokemon_id) DO UPDATE SET
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                base_experience = EXCLUDED.base_experience,
                species_id = EXCLUDED.species_id,
                sprite = EXCLUDED.sprite;
            """

    upsert_sql_pokemon_base_stats = """
            INSERT INTO pokemon_base_stats (pokemon_id, stat, value, effort)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (pokemon_id, stat) DO UPDATE SET
                value = EXCLUDED.value,
                effort = EXCLUDED.effort;
            """

    upsert_sql_pokemon_types = """
            INSERT INTO pokemon_types (pokemon_id, slot, type, type_id)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (pokemon_id, type_id) DO UPDATE SET
                slot = EXCLUDED.slot,
                type = EXCLUDED.type,
                type_id = EXCLUDED.type_id;

            """

    upsert_sql_pokemon_abilities = """
            INSERT INTO pokemon_abilities (pokemon_id, slot, ability, ability_id, is_hidden)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (pokemon_id, ability_id) DO UPDATE SET
                ability = EXCLUDED.ability,
                ability_id = EXCLUDED.ability_id,
                is_hidden = EXCLUDED.is_hidden;
            """

    upsert_sql_pokemon_moves = """
            INSERT INTO pokemon_moves (pokemon_id, move, move_id, level_learned_at, learn_method, sort_order, version_group)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (pokemon_id, move, move_id, level_learned_at, learn_method, sort_order, version_group) DO NOTHING;
            """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_pokemon_description, transformed_data['description'])
            await conn.executemany(upsert_sql_pokemon_base_stats, transformed_data['base_stats'])
            await conn.executemany(upsert_sql_pokemon_types, transformed_data['types'])
            await conn.executemany(upsert_sql_pokemon_abilities, transformed_data['abilities'])
            await conn.executemany(upsert_sql_pokemon_moves, transformed_data['moves'])
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting pokemon_id:{pokemon_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint1_transact(db_name: str, pokemon_ids: list[int], prog:Progress, task_id:int) -> None:
    """
    ETL from "https://pokeapi.co/api/v2/pokemon/{pokemon_id}/" endpoint
    and uploads to several tables in pokemon database:
    - pokemon_description
    - pokemon_base_stats
    - pokemon_types
    - pokemon_abilities
    - pokemon_moves

    Parameters
    ----------
    db_name : str
        name of database to load data into
    pokemon_ids: list[int]
       a list of Pokémon ids (eg. [1,151])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """

    if not pokemon_ids:
        return None


    for pid in pokemon_ids:
        try:
            url = f"https://pokeapi.co/api/v2/pokemon/{pid}/"
            data = await get_json(url)
            await apiendpoint1_to_db(db_name=db_name, pokemon_id=pid, data=data)
        except Exception as exc:
            logger.exception(f"species_id {pid} failed", exc_info=True)
        prog.advance(task_id, advance=1)





async def apiendpoint2_get_missing_species(db_name: str) -> list[int]:
    """
    generates the missing species ids based on values from the species table vs
    the species_description table. This descerns which species ids to call from
    the "https://pokeapi.co/api/v2/pokemon-species/{species_id}/" api

    Parameters
    ----------
    db_name : str
        name of database to load data into

    Returns
    -------
    list[int]
       a list of pokemon species ids (eg. [1,151])
    """
    query = """
        SELECT
            s.species_id
        FROM species AS s
        WHERE 1=1
            AND (s.species_id NOT IN (
            SELECT
                sd.species_id
            FROM species_description AS sd
        ))
    """

    async with get_connection(db_name=db_name) as conn:
        rows = await conn.fetch(query)

        # Flatten the list of 1‑element tuples into a plain list.
        return sorted([row[0] for row in rows])

def apiendpoint2_transform(data: dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/pokemon-species/{species_id}/" api

    Parameters
    ----------
    data: dict
        the json data generated from the api

    Returns
    -------
    dict
        {
            'description2': [(),()..],
            'flavor_text_entries': [(),()..]
        }
    """
    species_id = data['id']
    species_name = data['name']

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

    description = [(
        species_id,
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
                species_id,
                flavor_txt_orig,
                fte.get('version', {}).get('name', None),
            )
            flavor_text_entries.append(f)

    varieties = []
    for d in data['varieties']:
        is_default = d['is_default']
        pokemon_name = d['pokemon']['name']
        pokemon_id = int(d['pokemon']['url'].rsplit('/', 2)[-2])
        varieties.append((species_id, species_name, is_default, pokemon_id, pokemon_name))

    return {
        'description': description,
        'flavor_text_entries': flavor_text_entries,
        'varieties': varieties
    }

async def apiendpoint2_to_db(db_name: str, species_id: int, data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{species_id}/" endpoint
    and uploads to several tables in pokemon database:
    - species_description
    - species_flavor_text_entries

    Parameters
    ----------
    db_name : str
        name of database to load data into
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """
    transformed_data = apiendpoint2_transform(data)

    upsert_sql_description = """
        INSERT INTO species_description (species_id, genus, shape, habitat, color, evolves_from, base_happiness, hatch_counter, is_baby, is_legendary, is_mythical, capture_rate, growth_rate, gender_rate, has_gender_differences)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (species_id) DO UPDATE SET
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
        INSERT INTO species_flavor_texts (species_id, text, version)
        VALUES ($1,$2,$3)
        ON CONFLICT (species_id, version) DO UPDATE SET
            text = EXCLUDED.text,
            version = EXCLUDED.version;
        """

    upsert_sql_varieties = """
        INSERT INTO species_varieties (species_id, species_name, is_default, pokemon_id, pokemon_name)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (species_id, pokemon_id) DO UPDATE SET
            species_name = EXCLUDED.species_name,
            is_default = EXCLUDED.is_default,
            pokemon_name = EXCLUDED.pokemon_name;
        """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_description, transformed_data['description'])
            await conn.executemany(upsert_sql_flavor_text_entries, transformed_data['flavor_text_entries'])
            await conn.executemany(upsert_sql_varieties, transformed_data['varieties'])
            logger.info(f'Inserted Values Successfully: species_id:{species_id}')
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting species_id:{species_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint2_transact(db_name: str, species_ids: list[int], prog: Progress, task_id:int) -> None:
    """
    Transforms data from "https://pokeapi.co/api/v2/pokemon-species/{species_id}/" endpoint
    and uploads to several tables in pokemon database:
    - species_description
    - species_flavor_text_entries

    Parameters
    ----------
    db_name : str
        name of database to load data into
    species_ids: list[int]
       a list of pokemon species ids (eg. [1,151])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """
    if not species_ids:
        return None

    for sid in species_ids:
        try:
            url = f"https://pokeapi.co/api/v2/pokemon-species/{sid}/"
            data = await get_json(url)
            await apiendpoint2_to_db(db_name=db_name, species_id=sid, data=data)
        except Exception as exc:
            logger.exception(f"Species {sid} failed", exc_info=True)
        prog.advance(task_id, advance=1)




async def apiendpoint3_get_missing_move_ids(db_name: str) -> list[int]:
    """
    generates the missing move_ids based on values from the move vs
    the move_attributes table. This descerns which move_ids to call from
    the "https://pokeapi.co/api/v2/move/{move_id}/" api

    Parameters
    ----------
    db_name : str
        name of database to load data into

    Returns
    -------
    list[int]
       a list of pokemon move ids (eg. [1,4,51])
    """
    query = """
        SELECT
            m.move_id
        FROM move as m
        WHERE 1=1
            AND (m.move_id NOT IN (
            SELECT
                ma.move_id
            FROM move_attributes AS ma
            GROUP BY ma.move_id
        ))
        GROUP BY m.move_id;
    """

    async with get_connection(db_name=db_name) as conn:
        rows = await conn.fetch(query)

        return sorted([row[0] for row in rows])

def apiendpoint3_transform(data: dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/move/{move_id}/" api

    Parameters
    ----------
    db_name : str
        name of database to load data into
    data: dict
        the json data generated from the api

    Returns
    -------
    dict
        {
            'move_attributes': [(),()..]
        }
    """

    def first_item(seq, default=None):
        return seq[0] if seq else default

    id_ = int(data['id'])
    name = data.get('name', None)
    type_ = data.get('type', {}).get('name', None)
    damage_class = data.get('damage_class', {}).get('name', None)
    target = data.get('target', {}).get('name', None)
    accuracy = data.get('accuracy', None)
    power = data.get('power', None)
    pp = data.get('pp', None)
    effect_chance = data.get('effect_chance', None)
    effect_entries_first = first_item(data.get('effect_entries', {}), default={})
    effect_entry_long = effect_entries_first.get('effect', None)
    effect_entry_short = effect_entries_first.get('short_effect', None)

    meta = data.get("meta") or {}
    category = meta.get('category', {}).get('name', None)
    ailment = meta.get('ailment', {}).get('name', None)
    ailment_chance = meta.get('ailment_chance', None)
    crit_rate = meta.get('crit_rate', None)
    drain = meta.get('drain', None)
    flinch_chance = meta.get('flinch_chance', None)
    healing = meta.get('healing', None)
    stat_chance = meta.get('stat_chance', None)
    priority = data.get('priority', None)

    # replaces string 'none' with None
    vals = [
        id_,
        name, type_, damage_class,
        target, accuracy, power, pp,

        category, ailment, ailment_chance,
        crit_rate, drain, flinch_chance,
        healing, stat_chance, priority,
        effect_chance, effect_entry_short, effect_entry_long,
    ]

    new_vals = []
    for val in vals:
        if val == 'none':
            new_val = None
        else:
            new_val = val
        new_vals.append(new_val)

    return {
        'move_attributes': [tuple(new_vals)]
    }

async def apiendpoint3_to_db(db_name: str, move_id: int, data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/move/{mid}/" endpoint
    and uploads to table in pokemon database:
    - move_attributes

    Parameters
    ----------
    db_name : str
        name of database to load data into
    move_id: int
        move_id of the move
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """

    transformed_data = apiendpoint3_transform(data)

    upsert_sql_move_attributes = """
        INSERT INTO move_attributes (
            move_id, name, type, damage_class,
            target, accuracy, power, pp,
            category, ailment, ailment_chance, crit_rate,
            drain, flinch_chance, healing, stat_chance,
            priority, effect_chance, effect_entry_short, effect_entry_long
        )
        VALUES (
            $1,$2,$3,$4,
            $5,$6,$7,$8,
            $9,$10,$11,$12,
            $13,$14,$15,$16,
            $17,$18,$19,$20
        )
        ON CONFLICT (move_id) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            damage_class = EXCLUDED.damage_class,
            target = EXCLUDED.target,
            accuracy = EXCLUDED.accuracy,
            power = EXCLUDED.power,
            pp = EXCLUDED.pp,
            category = EXCLUDED.category,
            ailment = EXCLUDED.ailment,
            ailment_chance = EXCLUDED.ailment_chance,
            crit_rate = EXCLUDED.crit_rate,
            drain = EXCLUDED.drain,
            flinch_chance = EXCLUDED.flinch_chance,
            healing = EXCLUDED.healing,
            stat_chance = EXCLUDED.stat_chance,
            priority = EXCLUDED.priority,
            effect_chance = EXCLUDED.effect_chance,
            effect_entry_short = EXCLUDED.effect_entry_short,
            effect_entry_long = EXCLUDED.effect_entry_long;
        """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_move_attributes, transformed_data['move_attributes'])
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting species_id:{move_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint3_transact(db_name: str, move_ids: list[int], prog: Progress, task_id:int) -> None:
    """
    Transforms data from "https://pokeapi.co/api/v2/move/{mid}/" endpoint
    and uploads to table in pokemon database:
    - move_attributes

    Parameters
    ----------
    db_name : str
        name of database to load data into
    move_ids: list[int]
        a list of pokemon move ids (eg. [1,4,51])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """
    if not move_ids:
        return None


    for mid in move_ids:
        try:
            url = f"https://pokeapi.co/api/v2/move/{mid}/"
            data = await get_json(url)
            await apiendpoint3_to_db(db_name=db_name, move_id=mid, data=data)
        except Exception as exc:
            logger.exception(f"move {mid} failed", exc_info=True)
        prog.advance(task_id, advance=1)




async def apiendpoint4_get_missing_type_ids(db_name: str) -> list[int]:
    """
    generates the missing type_ids based on values from the type_ids table vs
    the type_rel table. This descerns which type_ids to call from
    the "https://pokeapi.co/api/v2/type/{type_id}/" api

    Parameters
    ----------
    db_name : str
        name of database to load data into

    Returns
    -------
    list[int]
       a list of pokemon type_id s (eg. [1,4,51])
    """
    query = """
        SELECT
            t.type_id
        FROM type AS t
        WHERE 1=1
            AND (t.type_id NOT IN (
            SELECT
                tdr.type_id
            FROM type_damage_relations AS tdr
            GROUP BY tdr.type_id
        ))
        GROUP BY t.type_id;
    """

    async with get_connection(db_name=db_name) as conn:
        rows = await conn.fetch(query)

        return sorted([row[0] for row in rows])

def apiendpoint4_transform(data: dict) -> dict[str, list[tuple[Any]]]:
    """
    transforms specific data received from the
    "https://pokeapi.co/api/v2/type/{type_id}/" api

    Parameters
    ----------
    data: dict
        the json data generated from the api

    Returns
    -------
    dict
        {
            'type_damage_relations': [(),()..]
            'type_sprites: [(),()..]
        }
    """

    type_ = int(data['id'])
    name = data['name']

    dmg_rel = data['damage_relations']
    dmg_double_from = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'double_from'
        ) for dr in dmg_rel['double_damage_from']
    ]
    dmg_double_to = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'double_to'
        ) for dr in dmg_rel['double_damage_to']
    ]
    dmg_half_from = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'half_from'
        ) for dr in dmg_rel['half_damage_from']
    ]
    dmg_half_to = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'half_to'
        ) for dr in dmg_rel['half_damage_to']
    ]
    dmg_no_from = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'no_from'
        ) for dr in dmg_rel['no_damage_from']
    ]
    dmg_no_to = [
        (
            type_,
            name,
            int(get_url_segment(dr['url'], segment=-1)),
            dr['name'],
            'no_to'
        ) for dr in dmg_rel['no_damage_to']
    ]
    dmg_rel_matrix = [
        dmg_double_from,
        dmg_double_to,
        dmg_half_from,
        dmg_half_to,
        dmg_no_from,
        dmg_no_to
    ]

    type_dmg_rel_flat = [elem for row in dmg_rel_matrix for elem in row]

    sprites = data['sprites']
    gens = list(sprites.keys())[::-1] #Newest generations first

    sprite_recent = None
    keep_going = True
    for gen in gens:
        version_groups = list(sprites[gen].keys())
        for vg in version_groups:
            sprite_recent = sprites[gen][vg]['name_icon']
            if sprite_recent:
                keep_going = False
                break
        if not keep_going:
            break

    type_sprite = [(type_, name, sprite_recent)]

    return {
        'type_damage_relations': type_dmg_rel_flat,
        'type_sprites': type_sprite
    }

async def apiendpoint4_to_db(db_name: str, type_id: int, data: dict) -> int:
    """
    Transforms data from "https://pokeapi.co/api/v2/type/{tid}/" endpoint
    and uploads to table in pokemon database:

    Parameters
    ----------
    db_name : str
        name of database to load data into
    type_id: int
        type_id of the type
    data: dict
        the json data generated from the api

    Returns
    -------
    int
        returns 1 if successful, 0 if not successful.
    """

    transformed_data = apiendpoint4_transform(data)

    upsert_sql_type_damage_relations = """
        INSERT INTO type_damage_relations (
            type_id, type_name, rel_type_id, rel_type_name,
            damage_label
        )
        VALUES (
            $1,$2,$3,$4,
            $5
        )
        ON CONFLICT (type_id, rel_type_id, damage_label) DO UPDATE SET
            type_name = EXCLUDED.type_name,
            rel_type_name = EXCLUDED.rel_type_name;
        """

    upsert_sql_type_sprites = """
        INSERT INTO type_sprites (
            type_id, type_name, type_sprite
        )
        VALUES (
            $1,$2,$3
        )
        ON CONFLICT (type_id) DO UPDATE SET
            type_name = EXCLUDED.type_name,
            type_sprite= EXCLUDED.type_sprite;
        """

    async with get_connection(db_name=db_name) as conn:
        try:
            await conn.executemany(upsert_sql_type_damage_relations, transformed_data['type_damage_relations'])
            await conn.executemany(upsert_sql_type_sprites, transformed_data['type_sprites'])
            return 1
        except Exception as exc:
            logger.exception(f"Issues upserting species_id:{type_id}.Error Message: {exc}", exc_info=True)
            return 0

async def apiendpoint4_transact(db_name: str, type_ids: list[int], prog: Progress, task_id:int) -> None:
    """
    Transforms data from "https://pokeapi.co/api/v2/type/{tid}/" endpoint
    and uploads to table in pokemon database:

    Parameters
    ----------
    db_name : str
        name of database to load data into
    type_ids: list[int]
        a list of type ids (eg. [1,4,51])
    prog: Progress
        progress object for progress bar
    task_id : int
        task id for progress bar
    """
    if not type_ids:
        return None

    for tid in type_ids:
        try:
            url = f"https://pokeapi.co/api/v2/type/{tid}/"
            data = await get_json(url)
            await apiendpoint4_to_db(db_name=db_name, type_id=tid, data=data)
        except Exception as exc:
            logger.exception(f"type_id {tid} failed", exc_info=True)
        prog.advance(task_id, advance=1)

def get_test_data():
    generations = [1]
    pokemon_urls = [
        f"https://pokeapi.co/api/v2/pokemon?offset={0}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={20}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={40}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={60}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={80}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={100}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={120}&limit={20}",
        f"https://pokeapi.co/api/v2/pokemon?offset={140}&limit={11}",
    ]
    return (generations, pokemon_urls)




async def main() -> None:
    """
    Orchestrates the three ETL stages respecting the dependency chain:
    0 → (1 & 2).
    """
    # -------------------------------------------------------------
    # STAGE 0: Run the generation stage first - it populates the
    # several the ids tables. a separate api needs to be
    # called to get the pokemon ids.
    # --------------------------------------------------------------
    if os.getenv('ENV') == 'prod':
        db_name = os.getenv("POSTGRES_DB")
        generations  = await apiendpoint0A_get_missing_generations(db_name=db_name)
        pokemon_urls = await apiendpoint0B_urls()
    else:
        db_name = 'test'
        delete_db(db_name=db_name)  # for quick reset for testing
        create_db(db_name=db_name)
        create_tables(db_name=db_name)
        generations, pokemon_urls = get_test_data()

    with create_progress_bar() as prog: #also helps docker with only one progress bar
        task_id_0A = prog.add_task("Fetching generations", total=len(generations))
        task_id_0B = prog.add_task("Fetching pokemon ids", total=len(pokemon_urls))

        await asyncio.gather(
            apiendpoint0A_transact(db_name=db_name, generations=generations, prog=prog, task_id=task_id_0A),
            apiendpoint0B_transact(db_name=db_name, urls=pokemon_urls, prog=prog, task_id=task_id_0B),
        )

    # --------------------------------------------------------------
    # Stage 1: After stage 0 is guaranteed to be complete, we can call
    # the rest of the APIs.
    # --------------------------------------------------------------
    species_ids_ep1 = await apiendpoint1_get_missing_pokemon(db_name=db_name)
    species_ids_ep2 = await apiendpoint2_get_missing_species(db_name=db_name)
    move_ids = await apiendpoint3_get_missing_move_ids(db_name=db_name)
    type_ids = await apiendpoint4_get_missing_type_ids(db_name=db_name)

    with create_progress_bar() as prog:
        task_id_1 = prog.add_task("Fetching Pokemon Details", total=len(species_ids_ep1))
        task_id_2 = prog.add_task("Fetching Species Details", total=len(species_ids_ep2))
        task_id_3 = prog.add_task("Fetching Move Details", total=len(move_ids))
        task_id_4 = prog.add_task("Fetching Type Details", total=len(type_ids))

        await asyncio.gather(
            apiendpoint1_transact(db_name=db_name,pokemon_ids=species_ids_ep1, prog=prog, task_id=task_id_1),
            apiendpoint2_transact(db_name=db_name,species_ids=species_ids_ep2, prog=prog, task_id=task_id_2),
            apiendpoint3_transact(db_name=db_name,move_ids=move_ids, prog=prog, task_id=task_id_3),
            apiendpoint4_transact(db_name=db_name,type_ids=type_ids, prog=prog, task_id=task_id_4),
        )

    await close_pool()



if __name__ == "__main__":
    asyncio.run(main()) # creates the event loop and shuts it down gracefully