from sqlalchemy import Table, Column, Integer, BigInteger, String, text
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from database.utils import metadata


def student_table(table_name: str) -> Table:
    return Table(
        table_name,
        metadata,
        Column("name", String(255), nullable=False),
    Column("roll_number", BigInteger, primary_key=True),
        Column("github_username", String(255)),
        Column("leetcode_username", String(255)),
        extend_existing=True,
    )


def create_student_table(engine: Engine, table_name: str) -> bool:
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        return False
    tbl = student_table(table_name)
    metadata.create_all(bind=engine, tables=[tbl])
    return True


def ensure_rollnumber_bigint(engine: Engine, table_name: str) -> None:
    """If table exists and roll_number is not BIGINT, alter it to BIGINT."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return
    cols = inspector.get_columns(table_name)
    rn = next((c for c in cols if c.get("name") == "roll_number"), None)
    if rn is None:
        return
    # SQLAlchemy doesn't provide a portable type name here; check via Python class name
    col_type = type(rn.get("type")).__name__.lower()
    if "bigint" in col_type or "big_integer" in col_type:
        return
    # Attempt to alter type to BIGINT (PostgreSQL syntax)
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{table_name}" ALTER COLUMN roll_number TYPE BIGINT'))
