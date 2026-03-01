import os

DEFAULT_DB_URL = "mysql+pymysql://root@localhost/nba_data"


def get_database_url() -> str:
    """Return SQLAlchemy DB URL from env, with backward-compatible default."""
    return os.getenv("NBA_DB_URL", DEFAULT_DB_URL)
