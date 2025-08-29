from utils.db_args import engine_args
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..config.Settings import settings


engine = create_engine(settings.DATABASE_URL, **engine_args())
Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()