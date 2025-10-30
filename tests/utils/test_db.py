import os
import tempfile
import sqlite3
import pytest
from lwflow.utils import Db  # replace with your actual import path


def test_init_with_valid_path_creates_connection(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    assert db.conn is not None
    db.close()


def test_init_raises_if_directory_does_not_exist():
    invalid_path = "/nonexistent_dir/test.db"
    with pytest.raises(FileNotFoundError):
        Db(invalid_path)


def test_execute_inserts_and_commits(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    create_table_query = "CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)"
    assert db.execute(create_table_query) is not None

    insert_query = "INSERT INTO test(name) VALUES (?)"
    cur = db.execute(insert_query, ("Alice",))
    assert cur is not None
    assert cur.lastrowid == 1

    # Verify insertion
    select_query = "SELECT * FROM test"
    results = db.query(select_query)
    assert results == [(1, "Alice")]

    db.close()


def test_execute_returns_none_on_sql_error(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    # This will fail because table does not exist
    result = db.execute("INSERT INTO missing_table VALUES (1)")
    assert result is None
    db.close()


def test_query_returns_results(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    db.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO test(name) VALUES (?)", ("Bob",))

    results = db.query("SELECT name FROM test WHERE id = ?", (1,))
    assert results == [("Bob",)]

    db.close()


def test_query_returns_empty_list_on_error(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    # Query on non-existing table returns empty list
    results = db.query("SELECT * FROM missing_table")
    assert results == []

    db.close()


def test_close_closes_connection(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))
    db.close()
    assert db.conn is None


def test_execute_raises_if_no_connection():
    db = Db.__new__(Db)  # create instance without calling __init__
    db.conn = None
    with pytest.raises(ConnectionError):
        db.execute("SELECT 1")


def test_context_manager_usage(tmp_path):
    db_file = tmp_path / "test.db"
    with Db(str(db_file)) as db:
        db.execute("CREATE TABLE test(id INTEGER PRIMARY KEY)")
        cur = db.execute("INSERT INTO test VALUES (1)")
        assert cur.lastrowid == 1
    # After context, connection should be closed
    assert db.conn is None


def test_foreign_key_enforcement(tmp_path):
    db_file = tmp_path / "test.db"
    db = Db(str(db_file))

    db.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))")
    db.execute("INSERT INTO parent(id) VALUES (1)")

    # This insert should succeed (valid foreign key)
    cur = db.execute("INSERT INTO child(id, parent_id) VALUES (?, ?)", (1, 1))
    assert cur is not None

    # This insert should fail (foreign key violation)
    result = db.execute("INSERT INTO child(id, parent_id) VALUES (?, ?)", (2, 999))
    assert result is None

    db.close()
