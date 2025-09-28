from typing import Optional, Tuple, List, Dict, Any
from sqlalchemy import Table, Column, String, BigInteger, PrimaryKeyConstraint, select, update, insert, delete
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from database.utils import metadata
from functions.tables import student_table


def notification_table() -> Table:
    return Table(
        "notification_Data",
        metadata,
        Column("name", String(255), keep_existing=True),
        Column("rollnumber", BigInteger, nullable=False, primary_key=True, keep_existing=True),
        Column("table_name", String(255), nullable=False, primary_key=True, keep_existing=True),
        Column("reason", String(1024), keep_existing=True),
        extend_existing=True,
    )


def create_notification_table(engine: Engine) -> bool:
    inspector = inspect(engine)
    if inspector.has_table("notification_Data"):
        return False
    tbl = notification_table()
    metadata.create_all(bind=engine, tables=[tbl])
    return True


def add_or_update_notification(engine: Engine, table_name: str, rollnumber: int, name: Optional[str], reason: str) -> None:
    tbl = notification_table()
    with engine.begin() as conn:
        res = conn.execute(
            update(tbl)
            .where(tbl.c.table_name == table_name, tbl.c.rollnumber == rollnumber)
            .values(name=name, reason=reason)
        )
        if res.rowcount == 0:
            conn.execute(insert(tbl).values(table_name=table_name, rollnumber=rollnumber, name=name, reason=reason))


def _resolve_student_by_roll(engine: Engine, rollnumber: int) -> Tuple[str, str]:
    """Return (table_name, name) for the first base table containing the rollnumber.
    Raises ValueError if not found in any base table.
    """
    inspector = inspect(engine)
    try:
        names = inspector.get_table_names()
    except Exception:
        names = inspector.get_table_names(schema="public")
    base_tables = [n for n in names if not n.endswith("_Data") and n != "notification_Data"]
    for t in base_tables:
        src = student_table(t)
        with engine.connect() as conn:
            row = conn.execute(select(src.c.name).where(src.c.roll_number == rollnumber)).first()
        if row:
            return t, row[0]
    raise ValueError(f"roll_number {rollnumber} not found in any base table")


def add_notification_by_roll(engine: Engine, rollnumber: int, reason: str) -> dict:
    table_name, name = _resolve_student_by_roll(engine, rollnumber)
    add_or_update_notification(engine, table_name, rollnumber, name, reason)
    return {"table_name": table_name, "rollnumber": rollnumber, "name": name, "reason": reason}


def remove_notification_by_roll(engine: Engine, rollnumber: int) -> int:
    tbl = notification_table()
    with engine.begin() as conn:
        res = conn.execute(delete(tbl).where(tbl.c.rollnumber == rollnumber))
        return res.rowcount or 0


def add_notification_for_table(engine: Engine, table_name: str, rollnumber: int, reason: str) -> dict:
    src = student_table(table_name)
    with engine.connect() as conn:
        row = conn.execute(select(src.c.name).where(src.c.roll_number == rollnumber)).first()
    if not row:
        raise ValueError(f"roll_number {rollnumber} not found in table '{table_name}'")
    name = row[0]
    add_or_update_notification(engine, table_name, rollnumber, name, reason)
    return {"table_name": table_name, "rollnumber": rollnumber, "name": name, "reason": reason}


def remove_notification(engine: Engine, table_name: str, rollnumber: int) -> int:
    tbl = notification_table()
    with engine.begin() as conn:
        res = conn.execute(delete(tbl).where(tbl.c.table_name == table_name, tbl.c.rollnumber == rollnumber))
        return res.rowcount or 0


def remove_notification_with_reason(engine: Engine, table_name: str, rollnumber: int, reason: str) -> int:
    tbl = notification_table()
    with engine.begin() as conn:
        res = conn.execute(
            delete(tbl).where(
                tbl.c.table_name == table_name,
                tbl.c.rollnumber == rollnumber,
                tbl.c.reason == reason,
            )
        )
        return res.rowcount or 0


def list_notifications(engine: Engine) -> List[Dict[str, Any]]:
    tbl = notification_table()
    with engine.connect() as conn:
        rows = conn.execute(select(tbl.c.name, tbl.c.rollnumber, tbl.c.table_name, tbl.c.reason).order_by(tbl.c.table_name, tbl.c.rollnumber)).mappings().all()
        return [dict(r) for r in rows]
