# n8n-nodes-sqlite3

This is an n8n community node. It lets you use SQLite3 in your n8n workflows.

SQLite3 is a lightweight, self-contained SQL database engine that stores all data in a single file and requires no server setup. It is ideal for embedded applications, local storage, and small to medium-sized projects.

[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

For local development:

```bash
npm run build
npm link
cd ~/.n8n/nodes
npm link n8n-nodes-sqlite3
```

## Credentials

The node uses a **SQLite** credential to store the database path. Configure it with an absolute path to your SQLite file:

```
/home/user/data/mydb.sqlite
```

The directory will be created automatically if it does not exist. The file will be created on first write if it does not exist.

## Operations

All operations are available under the **Database** resource.

| Operation | Description |
|-----------|-------------|
| **Select** | Query rows from a table with optional WHERE filters and column selection |
| **Insert** | Insert one or more rows into a table |
| **Update** | Update rows matching a WHERE condition |
| **Delete** | Delete rows matching a WHERE condition |
| **Upsert** | Insert or replace rows based on a conflict key |
| **Execute Query** | Run arbitrary SQL, including multi-statement scripts |

### Execute Query

Use this operation for DDL statements (`CREATE TABLE`, `DROP`, `ALTER`) or any SQL not covered by the structured operations. Multiple statements separated by `;` are all executed in a single call.

```sql
CREATE TABLE IF NOT EXISTS employees (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age  INTEGER
);
```

### Parameterized queries

When using **Execute Query**, use positional placeholders `$1`, `$2`, `$3`, … in the SQL and provide the corresponding values as a comma-separated string in **Options → Query Parameters**:

```sql
INSERT INTO employees (name, age) VALUES ($1, $2);
```

Query Parameters: `Alice, 30`

Placeholders are replaced in order — `$1` maps to the first value, `$2` to the second, and so on.

### Spread results

The **Select** operation has a **Spread Results** option. When enabled, each returned row becomes a separate n8n item instead of returning all rows as a single array item.

## Compatibility

Requires n8n with community node support. The native `better-sqlite3` binding is pre-compiled for the musl-based Docker image shipped by n8n (`node-v127-linux-musl-x64`). If you run n8n outside that environment you may need to rebuild the binding (see below).

## Building the native binding

The pre-built binary targets `node-v127-linux-musl-x64` (the default n8n Docker image). To rebuild for a different target:

```bash
docker build -t better-sqlite3-builder .

docker run --rm -it \
  -v ./.tmp:/app \
  -v ./native/node-v127-linux-musl-x64:/output \
  better-sqlite3-builder
```

Replace the output path with the appropriate ABI/platform directory for your target environment.

## Resources

- [n8n Community Nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
- [SQLite documentation](https://www.sqlite.org/docs.html)
- [better-sqlite3](https://github.com/WiseLibs/better-sqlite3)

## Contributing

Contributions are welcome. Open an issue or pull request on GitHub.
