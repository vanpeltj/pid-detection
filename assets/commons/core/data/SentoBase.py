import copy
import json
from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Any, Self

from sqlalchemy import (
    values,
    select,
    cast,
    case,
    column,
    literal,
    and_,
    or_,
    text,
    Executable,
    func,
    ARRAY,
    bindparam,
    JSON,
)
from sqlalchemy.dialects.postgresql import insert

from ..requests.request_manager import SentoRequest

request_manager = SentoRequest()

def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]

def to_dict(obj):
    if not hasattr(obj, "__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        element = []
        if isinstance(val, list):
            for item in val:
                element.append(to_dict(item))
        else:
            element = to_dict(val)
        result[key] = element
    return result


class classproperty:
    def __init__(self, func):
        self.fget = func

    def __get__(self, instance, owner):
        return self.fget(owner)

class SentoBaseData(ABC):
    """
        SentoBase class with some abstract methods
    """
    # request_manager = SentoRequest()
    # configure_sentry()

    def __init__(self):
        pass

    @property
    @abstractmethod
    def _primary_keys(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def _nullable_fields(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def _unique_fields(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def _non_unique_fields(self) -> list[str]:
        pass

    def getattr_or_null(self, k: str) -> Any:
        v = getattr(self, k)
        if not v and isinstance(v, Collection) and k in self._nullable_fields:
            return None
        return v

    @classmethod
    @abstractmethod
    def from_id(cls, id: int):
        return cls()

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def delete(self):
        pass

    def to_dict(self):
        return to_dict(self)

    @abstractmethod
    def to_create_dict(self):
        return dict()

    @abstractmethod
    def to_update_dict(self):
        return dict()

    def to_json(self):
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            default=str
        )

    def split(self, list_a, chunk_size):
        for i in range(0, len(list_a), chunk_size):
            yield list_a[i:i + chunk_size]

    @property
    @abstractmethod
    def _orm(self) -> type:
        pass

    @classproperty
    def unique_fields(self) -> list[str]:
        return self._unique_fields or self._primary_keys

    # Attribute storing the fields that should be updated if they are not None.
    # Notably used to retain the postprocess_scope in SentoArticleEntityFeature if it
    # has been set already and a new input is None.
    _sticky_fields: set[str] = set()

    @classmethod
    def make_upsert_statement(cls, items: list[Self]) -> (list[Executable], Executable):
        unique_fields = cls._unique_fields if cls._unique_fields else cls._primary_keys
        cols = unique_fields + cls._non_unique_fields
        # Yet another approach following https://klotzandrew.com/blog/postgres-passing-65535-parameter-limit,
        # avoiding the need of a custom type

        def render_record(x: tuple | list) -> str:
            """Render a record as a string. Enclosed with double quotes."""
            return f'"({",".join(str(i) for i in x)})"'

        def escape_scalar(x: Any) -> str:
            """Escape a scalar value with backslashes."""
            # See https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
            # curly braces, commas, double quotes, and backslashes must be escaped
            # ! We do not handle leading and trailing whitespaces, the word 'null' and
            # empty strings. Those will be ignored.
            return (
                str(x)
                .replace(r"{", r"\{")
                .replace(r"}", r"\}")
                .replace(r",", r"\,")
                .replace(r'"', r"\"")
                .replace(r"\\", r"\\\\")
            )

        def render_array(x: list) -> str:
            """Render a Postgresql array literal from a list. Handle separately scalars
            and tuples elements (composite types).
            Scalar elements are escaped with backslashes to allow input such as
            '"Fietspunt-Geel"~2' to be inserted.
            """
            return f"{{{','.join(render_record(i) if isinstance(i, (tuple, list)) else escape_scalar(i) for i in x)}}}"

        data_tuples = [
            tuple(
                (
                    render_array(v)
                    if ((v := x.getattr_or_null(k)) is not None)
                    and isinstance(cls._orm.__table__.c[k].type, ARRAY)
                    else v
                )
                for k in cols
            )
            for x in items
        ]
        flipped = list(zip(*data_tuples))
        input_rows = (
            text(
                "select "
                + ", ".join(
                    (
                        # Alternative to translate would be to have a custom function.
                        # See https://dba.stackexchange.com/a/54289
                        # f"translate({k}, '[]', '{{}}')::{t.item_type}[] AS {k}"
                        # Change 2024-11-09: Remove translate thanks to format array
                        # added above
                        f"{k}::{t.item_type}[] AS {k}"
                        if isinstance((t := cls._orm.__table__.c[k].type), ARRAY)
                        else k
                    )
                    for k in cols
                )
                + " from rows from("
                + ", ".join(
                    [
                        (
                            f"json_array_elements_text(:{k})"
                            if isinstance((t := cls._orm.__table__.c[k].type), ARRAY)
                            else f"unnest(:{k})"
                        )
                        for k in cols
                    ]
                )
                + f") as t({', '.join(cols)})"
                + f" order by {', '.join(unique_fields)}"
            )
            .bindparams(
                *[
                    bindparam(
                        k,
                        value=flip,
                        type_=(
                            JSON(none_as_null=True)
                            if isinstance((t := cls._orm.__table__.c[k].type), ARRAY)
                            else ARRAY(t)
                        ),
                    )
                    for k, flip in zip(cols, flipped)
                ]
            )
            .columns(*[cls._orm.__table__.c[k] for k in cols])
            .cte("input_rows")
        )
        # New approach to send all data into a single parameter (v). Required temporary
        # view to avoid anonymous record error.
        # if cls._orm.__table__.schema is not None:
        #     qualified_table_name = (
        #         f'"{cls._orm.__table__.schema }"."{cls._orm.__table__.name}"'
        #     )
        # else:
        #     qualified_table_name = f'"{cls._orm.__table__.name}"'
        # type_name = f"{cls._orm.__table__.name.lower()}_upsert_type"
        # type_stmt = text(
        #     f"CREATE OR REPLACE TEMP VIEW {type_name} AS select {', '.join([cls._orm.__table__.c[k].name for k in cols])} FROM {qualified_table_name}"
        # )
        # data_tuples = [
        #     tuple(
        #         (
        #             orjson_serializer(v)
        #             if isinstance((v := x.getattr_or_null(k)), dict)
        #             else v
        #         )
        #         for k in cols
        #     )
        #     for x in items
        # ]
        # input_rows = (
        #     text(
        #         f"SELECT (t.arr).* FROM (SELECT UNNEST( CAST(:v as {type_name}[]) ) AS arr) AS t"
        #     )
        #     .bindparams(v=data_tuples)
        #     .columns(*[cls._orm.__table__.c[k] for k in cols])
        #     .cte("input_rows")
        # )
        # Older approach that builds a VALUES clause.
        # data_tuples = [tuple(x.getattr_or_null(k) for k in cols) for x in items]
        # vals = values(*[cls._orm.__table__.c[k] for k in cols], name="vals").data(
        #     data_tuples
        # )
        # input_rows = select(*[cast(c, c.type) for c in vals.c]).cte("input_rows")
        inserted_rows = insert(cls._orm).from_select(input_rows.c, input_rows)
        inserted_rows = (
            inserted_rows.on_conflict_do_update(
                index_elements=unique_fields,
                set_={
                    **cls._orm._set_onupdate,
                    **{
                        k: (
                            v
                            if k not in cls._sticky_fields
                            else func.COALESCE(v, cls._orm.__table__.c[k])
                        )
                        for k, v in inserted_rows.excluded.items()
                        if k in cls._non_unique_fields
                    },
                },
                where=or_(
                    (
                        cls._orm.__table__.c[k].is_distinct_from(e)
                        if k not in cls._sticky_fields
                        else and_(
                            e.is_not(None),
                            cls._orm.__table__.c[k].is_distinct_from(e),
                        )
                    )
                    for k, e in reversed(inserted_rows.excluded.items())
                    if k in cls._non_unique_fields
                ),
            )
            .returning(
                case(
                    (
                        column("xmax") == literal(0),
                        literal("inserted"),
                    ),
                    else_=literal("updated"),
                ).label("source"),
                *(
                    getattr(cls._orm, k)
                    for k in [
                        *[kk for kk in cls._primary_keys if kk not in unique_fields],
                        *unique_fields,
                    ]
                ),
            )
            .cte("inserted_rows")
        )
        stmt = select(inserted_rows).union_all(
            select(
                literal("selected").label("source"),
                *[
                    getattr(cls._orm, k)
                    for k in cls._primary_keys
                    if k not in unique_fields
                ],
                *[input_rows.c[k] for k in unique_fields],
            )
            .join_from(
                input_rows,
                inserted_rows,
                and_(*[input_rows.c[k] == inserted_rows.c[k] for k in unique_fields]),
                isouter=True,
            )
            .where(
                # return non inserted rows
                getattr(inserted_rows.c, cls._primary_keys[0]).is_(None)
            )
            .join(
                cls._orm,
                and_(
                    *[input_rows.c[k] == cls._orm.__table__.c[k] for k in unique_fields]
                ),
            )
        )
        return [], stmt

    @classmethod
    def merge_instances(cls, a: Self, b: Self) -> Self:
        """Intended to be used to merge parent instances inside reduce() for bulk
        upsert"""
        a = copy.copy(a)
        for f in a._fields:
            if (fa := getattr(a, f)) == (fb := getattr(b, f)):
                continue
            if isinstance(fa, list) and isinstance(fb, list):
                fa.extend(fb)
                continue
            raise ValueError(f"Cannot merge {a} and {b}")
        return a