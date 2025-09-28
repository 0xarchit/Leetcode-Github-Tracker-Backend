from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional

from database.connection import engine
from database.utils import ensure_connectivity
from functions.tables import create_student_table
from functions.datatable import create_data_table
from functions.update_data import update_all_students
from functions.notification import (
    add_notification_for_table,
    remove_notification,
    create_notification_table,
    list_notifications,
)
from functions.data_fetch import build_json_list, SourceTableNotFound, DataTableNotFound
from functions.students import insert_student, upsert_student, TableNotFoundError
from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect, text
from datetime import datetime, timezone, timedelta
import os



PASSWORD = os.getenv("PASSWORD")


def require_password(password: str = Query(..., description="API password")):
    if password != PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")
    return True


app = FastAPI(title="Student DB API", version="1.0.0", dependencies=[Depends(require_password)])

# CORS: allow all origins/headers/methods for compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


class AddTableRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class AddStudentRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    name: str = Field(..., min_length=1, max_length=255)
    roll_number: int = Field(..., ge=0)
    github_username: Optional[str] = Field(default=None, max_length=255)
    leetcode_username: Optional[str] = Field(default=None, max_length=255)


class UpdateRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class DataRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class AddNotifRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    roll_number: int = Field(..., ge=0)
    reason: str = Field(..., min_length=1, max_length=1024)


class RemoveNotifRequest(BaseModel):
    table_name: str = Field(..., min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    roll_number: int = Field(..., ge=0)


@app.get("/health")
def health():
    ensure_connectivity(engine)
    return {"ok": True}


@app.post("/addtable", status_code=201)
def add_table(req: AddTableRequest):
    created = create_student_table(engine, req.table_name)
    if not created:
        raise HTTPException(status_code=409, detail=f"Table '{req.table_name}' already exists")
    return {"table": req.table_name, "created": True}


@app.post("/addDataTable", status_code=201)
def add_data_table(req: AddTableRequest):
    created = create_data_table(engine, req.table_name)
    if not created:
        raise HTTPException(status_code=409, detail=f"Table '{req.table_name}' already exists")
    return {"table": req.table_name, "created": True}


@app.post("/add", status_code=201)
def add_student(req: AddStudentRequest):
    try:
        # Use upsert so existing roll_number rows are updated
        row = upsert_student(
            engine,
            table_name=req.table_name,
            name=req.name,
            roll_number=req.roll_number,
            github_username=req.github_username,
            leetcode_username=req.leetcode_username,
        )
        return row
    except TableNotFoundError:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{req.table_name}' does not exist. Please call /addtable first.",
        )
    except IntegrityError as ie:
        # With upsert, this should be rare (e.g., other constraints). Surface as 400.
        raise HTTPException(status_code=400, detail=str(ie)) from ie


@app.post("/update")
def update_tables(req: UpdateRequest):
    source = req.table_name
    target = f"{source}_Data"
    try:
        updated, errors = update_all_students(engine, source, target)
        return {"source_table": source, "target_table": target, "updated": updated, "errors": errors}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data")
def get_data(req: DataRequest):
    try:
        data = build_json_list(engine, req.table_name)
        return data
    except SourceTableNotFound:
        raise HTTPException(status_code=400, detail=f"Table '{req.table_name}' does not exist")
    except DataTableNotFound:
        raise HTTPException(status_code=400, detail=f"Data table '{req.table_name}_Data' does not exist")


@app.post("/addNotif")
def add_notification(req: AddNotifRequest):
    try:
        create_notification_table(engine)
        result = add_notification_for_table(engine, req.table_name, req.roll_number, req.reason)
        return {"ok": True, **result}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/removeNotif")
def remove_notification_endpoint(req: RemoveNotifRequest):
    try:
        create_notification_table(engine)
        count = remove_notification(engine, req.table_name, req.roll_number)
        if count == 0:
            return {"ok": True, "removed": 0, "detail": "No notification found for roll number"}
        return {"ok": True, "removed": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/showNotif")
def show_notifications():
    try:
        create_notification_table(engine)
        return list_notifications(engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/available")
def available_tables():
    inspector = inspect(engine)
    try:
        names = inspector.get_table_names()
    except Exception:
        # Fallback for Postgres public schema
        names = inspector.get_table_names(schema="public")
    # Exclude data tables (suffix _Data)
    base_tables = sorted([n for n in names if not n.endswith("_Data")])
    return {"tables": base_tables}


@app.get("/lastUpdate")
def last_update():
    """Return rows from update_Data with changed_at converted to UTC+05:30."""
    IST = timezone(timedelta(hours=5, minutes=30))

    def to_ist_str(dt: object) -> str:
        if isinstance(dt, datetime):
            d = dt
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            d = d.astimezone(IST)
            # format with milliseconds
            ms = int(d.microsecond / 1000)
            return f"{d.strftime('%Y-%m-%d %H:%M:%S')}.{ms:03d}"
        return str(dt)

    queries = [
        'SELECT table_name, changed_at FROM "update_Data" ORDER BY changed_at DESC',
    ]
    last_err = None
    for q in queries:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(q)).all()
            return [
                {"table_name": r[0], "changed_at": to_ist_str(r[1])}
                for r in rows
            ]
        except Exception as e:
            last_err = e
            continue
    raise HTTPException(status_code=500, detail=str(last_err) if last_err else "Unable to read update_Data")
