# -------------------------------------------------------------------------
# SECTION: Importing Packages
# -------------------------------------------------------------------------


# PSL
import os
from pathlib import Path
import logging.config

# PYPI
import aiohttp
import asyncpg
import psycopg2
from contextlib import asynccontextmanager
from yaml import safe_load
from dotenv import load_dotenv
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn
)
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log
)

load_dotenv()

# -------------------------------------------------------------------------
# SECTION: SETUP LOGGING
# -------------------------------------------------------------------------
CONFIG_PATH = Path(os.getenv("LOG_CONFIG_PATH"))
with CONFIG_PATH.open("r") as f:
    cfg = safe_load(os.path.expandvars(f.read()))
logging.config.dictConfig(cfg)
fname = Path(__file__).name.replace(".py", "")
logger = logging.getLogger(fname)

# -------------------------------------------------------------------------
# SECTION: UTILITY FUNCTIONS
# -------------------------------------------------------------------------

def get_url_segment(url: str, segment: int = -1) -> str:
    """
    extracts a desired segment from a url. -1 is the last, -2 is the second to last

    Parameters
    ----------
    url : str
        URL that contains segments
    segment: int
        is the desired segment to capture in the url. Negative numbers are allowed.
        negative numbers reference the back segments first

    Returns
    -------
    str
        a string of the desired segment
    """
    if segment < 0 and url[-1] == '/':
        segment += -1
    return url.rsplit('/')[segment]

def create_progress_bar() -> Progress:
    """
    Build a pre‑configured `rich.progress.Progress` instance for ETL jobs.

    Returns
    -------
    rich.progress.Progress
        A new ``Progress`` object
    """
    return Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),  # auto‑size bar
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[bold magenta]{task.completed}/{task.total}"),
            "•",
            TimeRemainingColumn(),
    )

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
# SECTION: DECLARING POOL AND HELPERS
# init_pool – creates / retrieves the singleton pool.
# get_connection – obtains a connection from that pool via an async context manager.
# close_pool – gracefully shuts the pool down(usually called at program exit).
# -------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


async def init_pool(db_name) -> asyncpg.Pool:
    """
    Initialise (or retrieve) the global ``asyncpg`` connection pool.
    Subsequent calls return the already‑created pool.

    Used by get_connection()

    Returns
    -------
    asyncpg.Pool
        The (possibly newly created) connection pool.
    """
    global _pool
    if _pool is None:
        logger.debug("Creating asyncpg connection pool …")
        _pool = await asyncpg.create_pool(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT")),
            database=db_name,
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            min_size=1,
            max_size=10,          # tune to your workload
        )
    return _pool

@asynccontextmanager
async def get_connection(db_name) -> asyncpg.Connection:
    """
    Acquire a PostgreSQL connection from the global ``asyncpg`` pool.

    This helper abstracts the lifecycle of an ``asyncpg`` connection so callers
    can simply write::

        async with get_connection() as conn:
            await conn.fetch(...)

    Yields
    ------
    asyncpg.Connection
        An open connection drawn from the pool.
    """
    pool = await init_pool(db_name=db_name)

    async with pool.acquire() as conn:  # pool.acquire() is itself a context manager
        try:
            yield conn
        finally: # `finally` is kept for symmetry
            pass

async def close_pool() -> None:
    """
        Close the global asyncpg pool if it exists.
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None




# -------------------------------------------------------------------------
# SECTION: GENERAL UTILITY FUNCTIONS
# -------------------------------------------------------------------------

async def get_json(url: str, timeout: float = 10) -> dict:
    """
    Get JSON with exponential back‑off and Tenacity async retry.

    Parameters
    ----------
    url : str
        must point to a resource that returns a
        JSON payload (the function calls ``await resp.json()`` on a successful
        response).

    timeout : float, optional, default: 10s
        Total timeout for the request in seconds.

    Returns
    -------
    dict
        The parsed JSON object returned by the server.
    """
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


if __name__ == "__main__":

    pass

