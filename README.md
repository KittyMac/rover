# Rover

PostgreSQL and SQLite actor for Flynn.

Rover gives you Flynn-safe, actor-based access to a SQL database. The same
`Rover` / `Result` API works against either a PostgreSQL server or a SQLite
database — you choose the backend by the kind of connection info you hand it.

## Choosing a backend

```swift
// PostgreSQL
let pg = ConnectionInfoPostgres(host: "127.0.0.1",
                                username: "postgres",
                                password: "12345")
let rover = pg.newRover()

// SQLite (file-backed)
let sqliteFile = ConnectionInfoSQLite(path: "/var/data/app.sqlite3")
let rover = sqliteFile.newRover()

// SQLite (private, in-memory)
let sqliteMem = ConnectionInfoSQLite(path: nil)
let rover = sqliteMem.newRover()
```

Everything after that is identical regardless of backend:

```swift
rover.beConnect(info, Flynn.any) { success in
    // ...
}

rover.beRun("create table if not exists people ( id integer primary key, name text not null );",
            Flynn.any, Rover.ignore)

rover.beRun("insert into people (name) values ($1);", ["Rocco"], Flynn.any, Rover.ignore)

rover.beRun("select * from people order by name;", Flynn.any) { result in
    for row in 0..<result.rows {
        print(result.trimmed(hitch: row, 1) ?? "")
    }
}
```

`RoverManager` also accepts either connection info type and pools connections
the same way.

## What "transparent" means (and doesn't)

The *API* is transparent across backends; the *SQL dialect* is still yours to
manage. Rover does not rewrite your SQL, so a few things are worth knowing:

- **Parameters.** Both backends use positional `$1`, `$2`, … placeholders.
- **Arrays.** Postgres array predicates such as `name = ANY($1)` have no SQLite
  equivalent. Array parameters are bound to SQLite as a comma-joined string, so
  array-predicate SQL remains Postgres-only.
- **Types & DDL.** Postgres-isms like `serial`, `timestamptz`, or `char(n)`
  padding behave differently (or not at all) under SQLite. Use portable DDL
  (e.g. `integer primary key autoincrement`, `text`) if you want one schema to
  run on both.
- **COPY.** SQLite has no COPY protocol. `beCopy(toGzipFile:…)` accepts the
  Postgres `COPY … TO STDOUT [WITH (… HEADER)]` form, translates it to the
  equivalent `SELECT`, and writes the same gzip-compressed CSV that the
  Postgres backend produces. A plain `SELECT` is also accepted as-is.
- **In-memory databases.** A `nil` path opens a private `:memory:` database
  that lives only as long as that one connection. If you use `RoverManager`
  with more than one connection and want them to share an in-memory database,
  pass a shared-cache URI as the path instead, e.g.
  `ConnectionInfoSQLite(path: "file:shared?mode=memory&cache=shared")`.

## Platform notes

On Apple platforms SQLite is provided by the SDK's built-in `SQLite3` module.
On Linux you'll need the system libraries installed:

```
apt-get install libpq-dev libsqlite3-dev
```
