from typing import Optional, Dict, Any
from sqlalchemy import insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from functions.tables import student_table, ensure_rollnumber_bigint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert


class TableNotFoundError(Exception):
    pass


def insert_student(
    engine: Engine,
    *,
    table_name: str,
    name: str,
    roll_number: int,
    github_username: Optional[str],
    leetcode_username: Optional[str],
) -> Dict[str, Any]:
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        raise TableNotFoundError(table_name)

    tbl = student_table(table_name)
    # ensure schema can accept large roll numbers
    ensure_rollnumber_bigint(engine, table_name)
    with engine.begin() as conn:
        conn.execute(
            insert(tbl).values(
                name=name,
                roll_number=roll_number,
                github_username=github_username,
                leetcode_username=leetcode_username,
            )
        )
        row = conn.execute(
            select(
                tbl.c.name,
                tbl.c.roll_number,
                tbl.c.github_username,
                tbl.c.leetcode_username,
            ).where(tbl.c.roll_number == roll_number)
        ).mappings().first()

    return dict(row)


def upsert_student(
    engine: Engine,
    *,
    table_name: str,
    name: str,
    roll_number: int,
    github_username: Optional[str],
    leetcode_username: Optional[str],
) -> Dict[str, Any]:
    """Insert a student row or update existing row if roll_number already exists.

    Returns the resulting row as a dict.
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        raise TableNotFoundError(table_name)

    tbl = student_table(table_name)
    ensure_rollnumber_bigint(engine, table_name)

    with engine.begin() as conn:
        # Use Postgres ON CONFLICT upsert to avoid transaction aborts
        stmt = (
            pg_insert(tbl)
            .values(
                name=name,
                roll_number=roll_number,
                github_username=github_username,
                leetcode_username=leetcode_username,
            )
            .on_conflict_do_update(
                index_elements=[tbl.c.roll_number],
                set_=dict(
                    name=name,
                    github_username=github_username,
                    leetcode_username=leetcode_username,
                ),
            )
        )
        conn.execute(stmt)

        row = conn.execute(
            select(
                tbl.c.name,
                tbl.c.roll_number,
                tbl.c.github_username,
                tbl.c.leetcode_username,
            ).where(tbl.c.roll_number == roll_number)
        ).mappings().first()

    return dict(row)
