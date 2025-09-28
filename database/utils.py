from sqlalchemy import MetaData, text
from sqlalchemy.engine import Engine


metadata = MetaData()


def ensure_connectivity(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
