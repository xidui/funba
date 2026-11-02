"""PageView lookup migration preserves data and supports emergency pre-creation."""
import importlib.util
from io import StringIO
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa


@pytest.fixture
def migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/l9m0n1o2p3q4_index_pageview_request_lookups.py"
    spec = importlib.util.spec_from_file_location("pageview_lookup_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("precreated", [0, 1, 3])
def test_upgrade_and_downgrade_preserve_rows_and_existing_indexes(migration, precreated):
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE TABLE PageView (
                id INTEGER PRIMARY KEY, visitor_id VARCHAR(36) NOT NULL,
                path VARCHAR(500) NOT NULL, user_agent VARCHAR(500),
                is_crawler BOOLEAN NOT NULL, ip_address VARCHAR(45),
                created_at DATETIME NOT NULL
            )
        """)
        connection.exec_driver_sql("CREATE INDEX ix_PageView_created_at ON PageView (created_at)")
        connection.exec_driver_sql("""
            INSERT INTO PageView VALUES
                (1, 'visitor-a', '/news/1', 'browser', 0, '203.0.113.1', '2026-11-01 00:00:00'),
                (2, 'visitor-a', '/cn/news/1', 'browser', 0, '203.0.113.1', '2026-11-01 00:01:00'),
                (3, 'visitor-b', '/news/1', 'bot', 1, '203.0.113.2', '2026-11-01 00:02:00')
        """)
        original_rows = connection.exec_driver_sql("SELECT * FROM PageView ORDER BY id").all()
        for name, columns in migration._INDEXES[:precreated]:
            connection.exec_driver_sql(f"CREATE INDEX {name} ON PageView ({', '.join(columns)})")

        context = MigrationContext.configure(connection)
        with Operations.context(context):
            migration.upgrade()
            migration.upgrade()

        index_names = {index["name"] for index in sa.inspect(connection).get_indexes("PageView")}
        assert index_names == {"ix_PageView_created_at", *(name for name, _ in migration._INDEXES)}
        query = "SELECT COUNT(DISTINCT visitor_id) FROM PageView WHERE path IN ('/news/1', '/cn/news/1') AND is_crawler = 0"
        assert connection.exec_driver_sql(query).scalar_one() == 1
        plan = connection.exec_driver_sql("EXPLAIN QUERY PLAN " + query).all()
        assert any("COVERING INDEX ix_PageView_path_is_crawler_visitor_id" in str(row) for row in plan)

        with Operations.context(context):
            migration.downgrade()
        assert connection.exec_driver_sql("SELECT * FROM PageView ORDER BY id").all() == original_rows
        assert {index["name"] for index in sa.inspect(connection).get_indexes("PageView")} == {"ix_PageView_created_at"}
    engine.dispose()


@pytest.mark.parametrize("direction,operation", [("upgrade", "ADD INDEX"), ("downgrade", "DROP INDEX")])
def test_mysql_ddl_uses_one_online_alter(migration, direction, operation):
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="mysql", opts={"as_sql": True, "output_buffer": output},
    )
    with Operations.context(context):
        getattr(migration, direction)()
    sql = output.getvalue()
    assert sql.count("ALTER TABLE `PageView`") == 1
    assert sql.count(operation) == 3
    assert "ALGORITHM=INPLACE, LOCK=NONE" in sql
    if direction == "upgrade":
        assert "(`path`, `is_crawler`, `visitor_id`)" in sql
        assert "(`user_agent`, `created_at`)" in sql
        assert "(`ip_address`, `created_at`)" in sql
