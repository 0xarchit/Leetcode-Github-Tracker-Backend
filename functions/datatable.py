from sqlalchemy import Table, Column, BigInteger, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from database.utils import metadata


def data_table(table_name: str) -> Table:
    return Table(
        table_name,
        metadata,
        Column("rollnumber", BigInteger, primary_key=True),
        Column("git_followers", Integer),
        Column("git_following", Integer),
        Column("git_public_repo", Integer),
        Column("git_original_repo", Integer),
        Column("git_authored_repo", Integer),
        Column("last_commit_date", String(64)),
        Column("git_badges", String(1024)),
        Column("lc_total_solved", Integer),
        Column("lc_easy", Integer),
        Column("lc_medium", Integer),
        Column("lc_hard", Integer),
        Column("lc_ranking", BigInteger),
        Column("lc_lastsubmission", String(64)),
    Column("lc_lastacceptedsubmission", String(64)),
    Column("lc_cur_streak", Integer),
    Column("lc_max_streak", Integer),
        Column("lc_badges", String(1024)),
        Column("lc_language", String(1024)),
        Column("gh_contribution_history", JSONB),
        Column("lc_submission_history", JSONB),
        Column("lc_progress_history", JSONB),
        extend_existing=True,
    )


def create_data_table(engine: Engine, table_name: str) -> bool:
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        return False
    tbl = data_table(table_name)
    metadata.create_all(bind=engine, tables=[tbl])
    return True
