import pathlib
from crsql_correctness import connect, close, min_site_v, get_site_id

# c1


def test_min_site_version_on_init():
    c = connect(":memory:")
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v

def test_site_version_increments_on_modification():
    c = connect(":memory:")
    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.execute("insert into foo values (1, 2)")
    c.execute("commit")
    # +2 since create table statements bump version too
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 1
    c.execute("update foo set a = 3 where id = 1")
    c.execute("commit")
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 2
    c.execute("delete from foo where id = 1")
    c.execute("commit")
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 3
    close(c)

def test_site_version_restored_from_disk():
    dbfile = "./siteversion_c3.db"
    pathlib.Path(dbfile).unlink(missing_ok=True)
    c = connect(dbfile)

    # C3
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v

    # close and re-open to check that we work with empty clock tables
    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.close()
    c = connect(dbfile)
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v

    # insert so we get a clock entry
    c.execute("insert into foo values (1, 2)")
    c.commit()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == 1

    # Close and reopen to check that version was persisted and re-initialized correctly
    close(c)
    c = connect(dbfile)
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == 1
    close(c)

def test_each_tx_gets_a_site_version():
    c = connect(":memory:")

    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.commit()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 1

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.commit()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 2

    close(c)

def test_rollback_does_not_move_site_version():
    c = connect(":memory:")

    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")

    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.rollback()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.rollback()
    assert c.execute("SELECT crsql_site_version()").fetchone()[
        0] == min_site_v

    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.commit()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 1

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.commit()
    assert c.execute("SELECT crsql_site_version()").fetchone()[0] == min_site_v + 2

    close(c)

def sync_left_to_right(l, r, since):
    print("sync_left_to_right")
    changes = l.execute(
        "SELECT * FROM crsql_changes WHERE db_version > ?", (since,))
    for change in changes:
        print("sync_left_to_right: merging a change");
        r.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", change)
    r.commit()

def test_overwriting_keeps_track_of_true_site_version():
    def create_db1():
        db1 = connect(":memory:")
        db1.execute("CREATE TABLE foo (a PRIMARY KEY NOT NULL, b DEFAULT 0);")
        db1.execute("SELECT crsql_as_crr('foo');")
        db1.commit()
        return db1

    def create_db2():
        db2 = connect(":memory:")
        db2.execute("CREATE TABLE foo (a PRIMARY KEY NOT NULL, b DEFAULT 0);")
        db2.execute("SELECT crsql_as_crr('foo');")
        db2.commit()
        return db2
    
    db1 = create_db1()
    db2 = create_db2()

    db1.execute("INSERT INTO foo (a) VALUES (1);")
    db1.commit() # site_version = 1

    sync_left_to_right(db1, db2, 0)

    db1.execute("UPDATE foo SET b = 1;")
    db1.commit() # site_version = 2

    sync_left_to_right(db1, db2, 0)

    db2.execute("UPDATE foo SET b = 2;")
    db2.commit()

    sync_left_to_right(db2, db1, 0)

    changes = db1.execute("SELECT * FROM crsql_changes").fetchall()
    db1_site_id = get_site_id(db1)
    db2_site_id = get_site_id(db2)

    assert (changes == [('foo', b'\x01\t\x01', 'b', 2, 3, 3, db2_site_id, 1, 0, 1)])
    
    db1.execute("UPDATE foo SET b = 3;")
    db1.commit() # site_version = 3

    sync_left_to_right(db1, db2, 0)

    changes = db1.execute("SELECT * FROM crsql_changes").fetchall()

    assert (changes == [('foo', b'\x01\t\x01', 'b', 3, 4, 4, db1_site_id, 1, 0, 3)])

    assert db1.execute("SELECT * FROM crsql_site_versions").fetchall() == db2.execute("SELECT * FROM crsql_site_versions").fetchall()

    close(db1)
    close(db2)


