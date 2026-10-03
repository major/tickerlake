"""Behavior checks for the isolated PostgreSQL fixture contract."""

import psycopg


def test_owned_database_roles_are_isolated_and_least_privilege(pg_database):
    """Verify test roles and the database owner are isolated and non-superuser."""
    with psycopg.connect(pg_database.owner_dsn) as owner:
        assert owner.execute("SELECT current_database()").fetchone()[0] == pg_database.name
        assert owner.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0] is False
        assert (
            owner.execute(
                "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()"
            ).fetchone()[0]
            == "tickerlake_owner"
        )

    with psycopg.connect(pg_database.etl_dsn) as etl:
        assert etl.execute("SELECT pg_has_role(current_user, 'tickerlake_etl', 'member')").fetchone()[0]

    with psycopg.connect(pg_database.reader_dsn) as reader:
        assert reader.execute("SELECT pg_has_role(current_user, 'tickerlake_reader', 'member')").fetchone()[0]
        assert reader.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user").fetchone()[0] is False
