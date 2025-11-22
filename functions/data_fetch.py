from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import select, inspect
from sqlalchemy.engine import Engine
from functions.tables import student_table
from functions.datatable import data_table
import json


class SourceTableNotFound(Exception):
    pass


class DataTableNotFound(Exception):
    pass


def build_jsonl(engine: Engine, src_name: str, dst_name: Optional[str] = None) -> str:
    """Return NDJSON combining rows from src (students) and dst (data) by roll number.

    - If dst_name is None, uses f"{src_name}_Data".
    - Raises SourceTableNotFound or DataTableNotFound accordingly.
    """
    inspector = inspect(engine)
    if not inspector.has_table(src_name):
        raise SourceTableNotFound(src_name)
    dst_tbl_name = dst_name or f"{src_name}_Data"
    if not inspector.has_table(dst_tbl_name):
        raise DataTableNotFound(dst_tbl_name)

    src = student_table(src_name)
    dst = data_table(dst_tbl_name)

    # Left join so every student yields a line; data columns may be null
    stmt = (
        select(
            src.c.name.label("name"),
            src.c.roll_number.label("roll_number"),
            src.c.github_username.label("github_username"),
            src.c.leetcode_username.label("leetcode_username"),
            dst.c.git_followers,
            dst.c.git_following,
            dst.c.git_public_repo,
            dst.c.git_original_repo,
            dst.c.git_authored_repo,
            dst.c.last_commit_date,
            dst.c.git_badges,
            dst.c.lc_total_solved,
            dst.c.lc_easy,
            dst.c.lc_medium,
            dst.c.lc_hard,
            dst.c.lc_ranking,
            dst.c.lc_lastsubmission,
            dst.c.lc_lastacceptedsubmission,
                dst.c.lc_cur_streak,
                dst.c.lc_max_streak,
            dst.c.lc_badges,
            dst.c.lc_language,
            dst.c.gh_contribution_history,
            dst.c.lc_submission_history,
        )
        .select_from(src.outerjoin(dst, src.c.roll_number == dst.c.rollnumber))
        .order_by(src.c.roll_number)
    )

    lines: List[str] = []
    with engine.connect() as conn:
        for row in conn.execute(stmt).mappings():
            lines.append(json.dumps(dict(row)))

    return "\n".join(lines) + ("\n" if lines else "")


def build_json_list(engine: Engine, src_name: str, dst_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return a list of dicts combining rows from src and dst tables by roll number."""
    inspector = inspect(engine)
    if not inspector.has_table(src_name):
        raise SourceTableNotFound(src_name)
    dst_tbl_name = dst_name or f"{src_name}_Data"
    if not inspector.has_table(dst_tbl_name):
        raise DataTableNotFound(dst_tbl_name)

    src = student_table(src_name)
    dst = data_table(dst_tbl_name)

    stmt = (
        select(
            src.c.name.label("name"),
            src.c.roll_number.label("roll_number"),
            src.c.github_username.label("github_username"),
            src.c.leetcode_username.label("leetcode_username"),
            dst.c.git_followers,
            dst.c.git_following,
            dst.c.git_public_repo,
            dst.c.git_original_repo,
            dst.c.git_authored_repo,
            dst.c.last_commit_date,
            dst.c.git_badges,
            dst.c.lc_total_solved,
            dst.c.lc_easy,
            dst.c.lc_medium,
            dst.c.lc_hard,
            dst.c.lc_ranking,
            dst.c.lc_lastsubmission,
            dst.c.lc_lastacceptedsubmission,
                dst.c.lc_cur_streak,
                dst.c.lc_max_streak,
            dst.c.lc_badges,
            dst.c.lc_language,
            dst.c.gh_contribution_history,
            dst.c.lc_submission_history,
            dst.c.lc_progress_history,
        )
        .select_from(src.outerjoin(dst, src.c.roll_number == dst.c.rollnumber))
        .order_by(src.c.roll_number)
    )

    results: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        def _to_json_obj(v):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                return v
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return None
            return None

        for row in conn.execute(stmt).mappings():
            item = dict(row)
            # normalize history fields to JSON objects if stored as strings
            item["gh_contribution_history"] = _to_json_obj(item.get("gh_contribution_history"))
            item["lc_submission_history"] = _to_json_obj(item.get("lc_submission_history"))
            item["lc_progress_history"] = _to_json_obj(item.get("lc_progress_history"))
            item["lc_progress_history"] = _to_json_obj(item.get("lc_progress_history"))
            lcd = (item.get("last_commit_date") or "").strip()
            if lcd:
                try:
                    # Expecting ISO like 2025-08-28T15:41:29Z
                    item["last_commit_day"] = lcd.split("T")[0]
                except Exception:
                    item["last_commit_day"] = None
            else:
                item["last_commit_day"] = None
            results.append(item)

    return results
