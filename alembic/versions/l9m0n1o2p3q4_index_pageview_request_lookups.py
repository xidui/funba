"""Index PageView crawler lookups and news visitor counts.

Revision ID: l9m0n1o2p3q4
Revises: k8l9m0n1o2p3
"""
from alembic import op
import sqlalchemy as sa

revision = "l9m0n1o2p3q4"
down_revision = "k8l9m0n1o2p3"
branch_labels = None
depends_on = None

_INDEXES = (
    ("ix_PageView_ip_address_created_at", ("ip_address", "created_at")),
    ("ix_PageView_user_agent_created_at", ("user_agent", "created_at")),
    ("ix_PageView_path_is_crawler_visitor_id", ("path", "is_crawler", "visitor_id")),
)


def _existing_indexes():
    if op.get_context().as_sql:
        return {}
    return {
        index["name"]: tuple(index["column_names"])
        for index in sa.inspect(op.get_bind()).get_indexes("PageView")
    }


def _alter_mysql(clauses):
    # Build all indexes in one pass while allowing concurrent reads/writes.
    # Fail instead of falling back to a table-copying or locking algorithm.
    statement = "ALTER TABLE `PageView` " + ", ".join(clauses)
    statement += ", ALGORITHM=INPLACE, LOCK=NONE"
    if op.get_context().as_sql:
        op.execute(statement)
        return

    connection = op.get_bind()
    previous_timeout = connection.execute(sa.text("SELECT @@SESSION.lock_wait_timeout")).scalar_one()
    try:
        # An old reader can otherwise block the metadata lock and queue new
        # application queries behind this ALTER for an unbounded time.
        connection.execute(sa.text("SET SESSION lock_wait_timeout = 10"))
        op.execute(statement)
    finally:
        connection.execute(sa.text(f"SET SESSION lock_wait_timeout = {int(previous_timeout)}"))


def upgrade() -> None:
    existing = _existing_indexes()
    missing = []
    for name, columns in _INDEXES:
        if name in existing:
            if existing[name] != columns:
                raise RuntimeError(f"PageView index {name} has unexpected columns: {existing[name]}")
        else:
            missing.append((name, columns))
    if not missing:
        return

    if op.get_bind().dialect.name == "mysql":
        # Full varchar(500) keys fit modern InnoDB's 3072-byte limit with
        # utf8mb4. Keep the full path so the news query can use a covering index.
        _alter_mysql([
            f"ADD INDEX `{name}` (" + ", ".join(f"`{column}`" for column in columns) + ")"
            for name, columns in missing
        ])
    else:
        for name, columns in missing:
            op.create_index(name, "PageView", list(columns))


def downgrade() -> None:
    existing = _existing_indexes()
    names = [name for name, _ in reversed(_INDEXES) if op.get_context().as_sql or name in existing]
    if not names:
        return
    if op.get_bind().dialect.name == "mysql":
        _alter_mysql([f"DROP INDEX `{name}`" for name in names])
    else:
        for name in names:
            op.drop_index(name, table_name="PageView")
