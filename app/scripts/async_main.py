import pathlib
import sys
import os
from pathlib import Path
import asyncio
# print('current direrere: ', os.getcwd())

from dotenv import load_dotenv
load_dotenv()
# test = os.getenv('PYTHON_SCRIPTS')
# print('helllllo',test)
# print(sorted(Path(os.getcwd()).rglob('*')))
# print(os.getenv('PYTHON_SCRIPTS'))

sys.path.append(os.getenv('PYTHON_SCRIPTS'))
import initialize_db as idb
import async_poke_api_etl as apa



async def main() -> None:
    idb.delete_db()
    idb.create_db()
    idb.create_tables()

    generations = await apa.apiendpoint0_get_missing_generations()
    await apa.apiendpoint0_transact(generations)   

    # --------------------------------------------------------------
    #   After stage‑0 is guaranteed to be complete, we can safely
    #    ask the DB for the IDs each downstream job still needs.
    # --------------------------------------------------------------
    poke_ids_ep1 = await apa.apiendpoint1_get_missing_poke_ids()
    poke_ids_ep2 = await apa.apiendpoint2_get_missing_poke_ids()

    # --------------------------------------------------------------
    #    Launch the two independent stages concurrently.
    #    `asyncio.gather` runs both coroutines in parallel and
    #    returns when *both* have finished.
    # --------------------------------------------------------------
    await asyncio.gather(
        apa.apiendpoint1_transact(poke_ids_ep1),
        apa.apiendpoint2_transact(poke_ids_ep2),
    )

    # --------------------------------------------------------------
    #   Clean‑up: close the asyncpg connection pool.
    # --------------------------------------------------------------
    if apa._pool:                     # `_pool` is the module‑level asyncpg pool
        await apa._pool.close()

if __name__ == "__main__":
    # `asyncio.run` creates the event loop, runs `main`, and shuts the loop down.
    asyncio.run(main())