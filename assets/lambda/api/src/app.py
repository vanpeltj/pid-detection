import os
import typing
import orjson
from fastapi import FastAPI
from mangum import Mangum
from fastapi_sqlalchemy import DBSessionMiddleware
from starlette.responses import JSONResponse
from endpoints.api_router import api_router
from core.config.Settings import settings

ROOT_PATH = os.getenv("ROOT_PATH", "/prod")


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return orjson.dumps(content)


app = FastAPI(
    title ="PID FastAPI Application",
    root_path=ROOT_PATH,  # Set the root path for API Gateway
    version="1.0.0",
    redoc_url=None,
    default_response_class=ORJSONResponse,
)

def orjson_serializer(obj):
    """
        Note that `orjson.dumps()` return byte array, while sqlalchemy expects string, thus `decode()` call.
    """
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC).decode()


app.add_middleware(DBSessionMiddleware, db_url=settings.DATABASE_URL, engine_args={'json_serializer': orjson_serializer})
# app.add_middleware(SentryAsgiMiddleware)
app.include_router(api_router)


@app.get("/health")
def health():
    return "ok"

# Mangum adapter for Lambda
handler = Mangum(app, api_gateway_base_path=ROOT_PATH)