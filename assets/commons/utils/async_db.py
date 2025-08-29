import asyncio
import os
import time
import orjson

from contextlib import asynccontextmanager
from typing import Literal

from .db_args import connection_string, engine_args
from .logger import makeCustomLogger
from sqlalchemy import AsyncAdaptedQueuePool, inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.util import greenlet_spawn

_logger = makeCustomLogger(__name__)


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}


def orjson_serializer(obj):
    """
    Note that `orjson.dumps()` return byte array, while sqlalchemy expects string, thus `decode()` call.
    """
    return orjson.dumps(
        obj, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC
    ).decode()
    # return orjson.dumps(obj).decode()


from core.config.Settings import settings

indexingAsyncSession = async_sessionmaker(autoflush=False, expire_on_commit=False)

readOnlyIndexingAsyncSession = async_sessionmaker(
    autoflush=False, expire_on_commit=False
)



async def warm_pool(engine: AsyncEngine):
    pool: AsyncAdaptedQueuePool = engine.pool

    # Initialize new connections up to half the max pool size
    while pool.checkedout() + pool.checkedin() < pool.size():
        if pool._inc_overflow():
            conn = await greenlet_spawn(pool._create_connection)
            await greenlet_spawn(pool._do_return_conn, conn)
            _logger.debug(f"Created connection in warmer task.")
        await asyncio.sleep(5)  # Create connections gradually

    # Keep connections warm with periodic select
    sleep_time = max(1, 30 / max(float(os.getenv("CONCURRENCY_MULTIPLIER", 1)), 1))
    while True:
        async with engine.connect() as conn:
            # Check age of the connection
            _fairy = conn.sync_connection._dbapi_connection
            _record = _fairy._connection_record
            if (
                pool._recycle > -1
                and time.time() - _record.starttime > pool._recycle / 2
            ):
                # Force overflow by creating a new connection
                # Upon closing, the pool will discard the old connection
                conn = await greenlet_spawn(pool._create_connection)
                await greenlet_spawn(pool._do_return_conn, conn)
                pool._overflow += 1  # Simulate overflow
                _logger.debug("Re-created connection by warmer.")
            else:
                _logger.debug("Checked out connection by warmer.")
                await conn.execute(text("SELECT 1"))
        await asyncio.sleep(sleep_time)  # Check every 60 seconds


@asynccontextmanager
async def use_async_engine(
    driver="psycopg_async",
    global_sessionmaker: bool | Literal["overwrite"] = True,
    pool_warmer: bool = False,
    **kwargs,
) -> AsyncEngine:
    global indexingAsyncSession
    args = engine_args(driver=driver, async_=True, **kwargs)
    if pool_warmer:
        assert (
            args.get("pool_use_lifo") is not True
        ), "Pool warmer requires FIFO queue, set pool_use_lifo=False"
    asyncEngine = create_async_engine(
        connection_string(settings.DATABASE_HOST, db="indexing", driver=driver),
        **args,
    )
    replica_args = args.copy()
    if "execution_options" not in replica_args:
        replica_args["execution_options"] = {}
    replica_args["execution_options"]["postgresql_readonly"] = True
    replicaAsyncEngine = create_async_engine(
        connection_string(settings.DATABASE_REPLICA_HOST, db="indexing", driver=driver),
        **replica_args,
    )
    _logger.info(
        f"Created async engine (write: {asyncEngine.url.host}, read: {replicaAsyncEngine.url.host}). {asyncEngine.pool.status()}"
    )
    if global_sessionmaker is True or global_sessionmaker == "overwrite":
        if indexingAsyncSession.kw.get("bind") and global_sessionmaker != "overwrite":
            raise ValueError("Global sessionmaker already bound to an engine.")
        indexingAsyncSession.configure(bind=asyncEngine)
        if (
            readOnlyIndexingAsyncSession.kw.get("bind")
            and global_sessionmaker != "overwrite"
        ):
            raise ValueError("Global sessionmaker already bound to an engine.")
        readOnlyIndexingAsyncSession.configure(bind=replicaAsyncEngine)

    warmer_tasks = []
    if pool_warmer:
        warmer_tasks.append(asyncio.create_task(warm_pool(asyncEngine)))
        warmer_tasks.append(asyncio.create_task(warm_pool(replicaAsyncEngine)))
        _logger.info(
            f"Started {len(warmer_tasks)} warmer tasks initialize and keep warm connections to db."
        )

    try:
        # asyncEngine._replica_engine = replicaAsyncEngine  # Doesn't work
        yield asyncEngine
    finally:
        for task in warmer_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                _logger.debug("Warmer task cancelled.")

        if global_sessionmaker is True or global_sessionmaker == "overwrite":
            if indexingAsyncSession.kw.get("bind") == asyncEngine:
                indexingAsyncSession.configure(bind=None)
            if readOnlyIndexingAsyncSession.kw.get("bind") == replicaAsyncEngine:
                readOnlyIndexingAsyncSession.configure(bind=None)
        await asyncio.gather(asyncEngine.dispose(), replicaAsyncEngine.dispose())
        _logger.info(f"Disposed of async engine. Status: {asyncEngine.pool.status()}")


async def get_async_db():
    async with indexingAsyncSession() as db:
        yield db



async def analyze_all(driver="psycopg_async"):
    ## Useful after blue/green deployment
    import asyncio
    import os

    os.environ["CONCURRENCY_MULTIPLIER"] = "4"
    from sqlalchemy import text

    args = engine_args(driver=driver, async_=True, isolation_level="AUTOCOMMIT")
    print(args)
    engine = create_async_engine(
        connection_string(settings.DATABASE_HOST_REPLICA, db="indexing", driver=driver),
        **args,
    )
    try:
        async with engine.connect() as conn:
            r = await conn.execute(
                text(
                    "SELECT schemaname, tablename FROM pg_catalog.pg_tables where schemaname = 'public'"
                )
            )
            r = r.all()
            print(r)

        sem = asyncio.Semaphore(args.get("pool_size", 5))

        async def analyze_table(schema, table):
            async with sem, engine.connect() as conn:
                await conn.execute(text(f'ANALYZE {schema}."{table}"'))
                _logger.info(f"Analyzed {schema}.{table}")

        async with asyncio.TaskGroup() as tg:
            for schema, table in r:
                tg.create_task(analyze_table(schema, table))

    finally:
        await engine.dispose()


# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(analyze_all())