import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()


def _normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        # explicitly select psycopg2 driver
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    return url


raw_url = os.getenv("POSTGRES_CONNECT_STRING")
if not raw_url:
    raise RuntimeError("POSTGRES_CONNECT_STRING not set in environment/.env")
DATABASE_URL = _normalize_db_url(raw_url)

# SQLAlchemy 2.x prefers postgresql[+driver] scheme; normalize common 'postgres://' URLs.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    # Explicitly add psycopg2 driver if not present
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

# Pool settings tuned for simple API usage
# Allow tuning via env vars; set higher defaults for Neon pooler
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "40"))

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    future=True,
)
