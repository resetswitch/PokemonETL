import sys
import os
from typing import Callable, Any

from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.getenv('PATH_PROJECT_DIR').join('scripts'))
import initialize_db
import poke_api_etl


def apply(func: Callable, func_to_call: Callable, max_attempts:int=5, delay:int=2, backoff_factor=2) -> Any:

    attempt = 0

    arg = func_to_call()
    while (attempt < max_attempts) and (arg != []):
        attempt += 1
        func(arg)
        arg = func_to_call()

        # ----- Retry logic -------------------------------------------------
        if (attempt == max_attempts):
            break  # Exhausted attempts

        # Apply exponential back‑off
        print(f"→ Sleeping {sleep_time:.2f}s before next try")
        time.sleep(delay)
        delay *= backoff_factor   # exponential growth


initialize_db.create_db()
initialize_db.create_tables()

generations = poke_api_etl.apiendpoint0_get_missing_generations()
poke_api_etl.apiendpoint0_transact(generations)

poke_ids_apiep1 = poke_api_etl.apiendpoint1_get_missing_poke_ids()
poke_api_etl.apiendpoint1_transact(poke_ids_apiep1)

poke_ids_apiep2 = poke_api_etl.apiendpoint2_get_missing_poke_ids()
poke_api_etl.apiendpoint2_transact(poke_ids_apiep2)