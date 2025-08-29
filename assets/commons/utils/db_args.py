import os
from typing import Protocol
from collections import deque

import orjson

from enum import Enum
from types import GeneratorType

def minimal_jsonable_encoder(obj):
    """
    Inspired by fastapi.encoders.jsonable_encoder.
    Mainly used for handling serialization of StrEnum that are not allowed as dict keys
    in orjson.
    As with SQLAlchemy, we only care about the Enum name. Enum values are irrelevant and
    not persisted in the database.
    """
    if isinstance(obj, Enum):
        return obj.name
    elif isinstance(obj, dict):
        encoded_dict = {}
        for key, value in obj.items():
            encoded_key = minimal_jsonable_encoder(
                key,
            )
            encoded_value = minimal_jsonable_encoder(
                value,
            )
            encoded_dict[encoded_key] = encoded_value
        return encoded_dict
    elif isinstance(obj, (list, set, frozenset, GeneratorType, tuple, deque)):
        encoded_list = []
        for item in obj:
            encoded_list.append(
                minimal_jsonable_encoder(
                    item,
                )
            )
        return encoded_list
    else:
        return obj

def orjson_serializer(obj):
    """
    Note that `orjson.dumps()` return byte array, while sqlalchemy expects string, thus `decode()` call.
    """
    return orjson.dumps(
        minimal_jsonable_encoder(obj),
        option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC,
    ).decode()


def connection_string(host, db="data", driver="psycopg"):
    return f"postgresql+{driver}://{host}/{db}"


class ConnectionString(Protocol):
    def __call__(self, db: str, driver: str) -> str: ...


default_application_name = (
    f"{os.getenv('APPNAME', 'local')}-{os.getenv('ENVIRONMENT', 'dev')}"
)


def psycopg_connect_args(appname: str) -> dict:
    return {
        "fallback_application_name ": appname,
    }


def asyncpg_connect_args(appname: str) -> dict:
    return {
        "server_settings": {
            "application_name": appname,
        },
    }


def default_engine_args(connections_multiplier: float | None = None):
    if connections_multiplier is None:
        connections_multiplier = 1
    effective_multiplier = (
        float(os.getenv("CONCURRENCY_MULTIPLIER", 1)) * connections_multiplier
    )
    return {
        "json_serializer": orjson_serializer,
        "json_deserializer": orjson.loads,
        "pool_pre_ping": False,
        "pool_recycle": 3600,
        "pool_use_lifo": True,
        "pool_size": 10 * effective_multiplier,
        "pool_timeout": 60,
        "max_overflow": 0,  # No overflow (creating connections isn't free)
    }


def engine_args(
    driver="psycopg",
    async_: bool = False,
    connect_args: dict = None,
    connections_multiplier: float = None,
    appname=None,
    **kwargs,
) -> dict:
    if appname is None:
        appname = default_application_name
        if async_:
            appname = "async-" + appname
    if connect_args is None:
        connect_args = {}
    if "psycopg" in driver:
        connect_args = {**psycopg_connect_args(appname), **connect_args}
    else:
        connect_args = {**asyncpg_connect_args(appname), **connect_args}

    return {
        **default_engine_args(connections_multiplier),
        "connect_args": connect_args,
        **kwargs,
    }