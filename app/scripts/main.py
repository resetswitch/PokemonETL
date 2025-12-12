# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------

# PYSL
import os

# PYPI
import asyncio
from dotenv import load_dotenv

# Project
from db_schema import (
    create_db,
    delete_db,
    create_tables,
    add_constraints
)
from etl import (
    apiendpoint0A_get_missing_generations,
    apiendpoint0A_transact,
    apiendpoint0B_urls,
    apiendpoint0B_transact,
    apiendpoint1_get_missing_pokemon,
    apiendpoint1_transact,
    apiendpoint2_get_missing_species,
    apiendpoint2_transact,
    apiendpoint3_get_missing_move_ids,
    apiendpoint3_transact,
    apiendpoint4_get_missing_type_ids,
    apiendpoint4_transact,
    get_test_data,
)
from utility import (
    create_progress_bar,
    close_pool
)


load_dotenv()
print(f'PROJECT_DIR_PATH: {os.getenv('PROJECT_DIR_PATH')}')


async def main() -> None:
    # ------------------------------------------------------------------------
    # Stage I: create the database and tables
    # ------------------------------------------------------------------------

    db_name = os.getenv("POSTGRES_DB")
    # delete_db(db_name=db_name) # for quick reset for testing
    print('Creating Database: ', db_name)
    create_db(db_name=db_name)
    print('Created Database: ', db_name)
    print('Creating Tables for Database: ', db_name)
    create_tables(db_name=db_name)
    print('Created Tables for Database: ', db_name)

    # ------------------------------------------------------------------------
    # Stage II: generate the missing ids from core data tables. and
    # any that are missing run apiendpoint0A and 0B and wait for group to
    # finish
    # ------------------------------------------------------------------------

    # TODO: make apiendpoint0B_urls not re_run in its entirety, everytime it is ran
    print('Beginning ETL for Database: ', db_name)
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

    # ------------------------------------------------------------------------
    # Stage III: generate the missing ids for details tables. Then
    # apiendpoint1,2,3,4 will run concurrently and wait for all to
    # finish
    # ------------------------------------------------------------------------

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


    await close_pool() # close the asyncpg connection pool

    # ------------------------------------------------------------------------
    # Stage IV: Apply Constraints. Constraints could not be put in at table
    # creation due to parent  child relationships not matching during async (concurrency)
    # ------------------------------------------------------------------------

    print('Adding Constraints..')
    add_constraints(db_name=db_name)
    print('Added Constraints')

    print(f'ETL to database {db_name} is now complete.')
if __name__ == "__main__":
    asyncio.run(main())