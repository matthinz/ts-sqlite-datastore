# ts-sqlite-datastore

Version: 0.0.1

This is a single file, Promise-oriented API for working with a local sqlite
database in Node.js.

## Column types

sqlite represents data using four types:

- `TEXT`
- `BLOB`
- `INTEGER`
- `REAL`

These four map *somewhat* cleanly onto Javascript types. Values can also be
NULL, but we're going to disregard that for now.

The `JsTypeForSqliteNativeType` helper allows us to convert between sqlite types
and Javascript types, e.g.:

```ts
type T = JsTypeForSqliteNativeType<"TEXT", false>; // string
type NullableT = JsTypeForSqliteNativeType<"TEXT", true>; // string | null
```

## Schemas

A `Schema` is Javascript object that describes the tables in your database.

Here's an example of a Schema that defines a single table, `users`:

```ts
const SCHEMA = {
  tables: {
    users: {
      columns: {
        id: {
          type: "INTEGER",
          autoIncrement: true,
        },
        name: "TEXT",
      },
      primaryKey: "id"
    }
  }
} satisfies Schema;
```

(The `satisfies Schema` is important.)

Each table is composed of columns. Each column has, at a minimum, a name and a type.

The simplest column definition looks like this:

```ts
{
  columns: {
    // Store name as a non-nullable string value
    "name": "TEXT"
  }
}
```

You can also provide a more detailed column definition, like this:

```ts
{
  columns: {
    "birthdate": {
      type: "TEXT",
      nullable: true,
      // When reading from the database, translate strings into Date objects
      parse: (value) => value == null ? null : new Date(value as string),
      // When writing to the database, translate Date objects into strings
      serialize: (value) => value == null ? null : (value as Date).toISOString(),
    }
}
```

Here's the full set of properties you can use to describe your columns:

| Property       | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| `type`         | The type of the column (one of the four native types)                       |
| `autoIncrement`| Whether this column's value should auto-increment (only valid for `INTEGER` columns that are also primary keys) |
| `defaultValue` | A value to insert if none is provided                                       |
| `nullable`     | Whether the column can contain NULL values (defaults to `false`)            |
| `parse`        | (Optional.) A function to parse data from the database into a Javascript value |
| `serialize`    | (Optional.) A function to serialize a Javascript value into a form suitable for the database |
| `unique`       | Whether values in this column must be unique (defaults to `false`)          |

To derive a Javascript type for a record in a table, you can use
`RecordFor<Table>`.

## Creating a SqliteDatastore

The `SqliteDatastore` constructor an options object with the following properties:

| Property | Type | Description |
| -- | -- | -- |
| `schema` | `Schema` | The schema for the database (required). |
| `filename` | `string` | The name of the sqlite database file to open. If not provided, an in-memory database will be used. |
| `onDatabaseReady` | `(err: Error | null, db?: Database) => void` | A hook to allow the caller to obtain the underlying Database instance we are working with. (This is intended for internal use only.) |

## Inserting records

We want to be able to cover a few different insertion scenarios:

1. Insert a single record
2. Insert multiple records

After we insert records, it should be possible to:

- Get the number of records inserted
- Get the IDs of records that were auto-incremented

However, if the number of records inserted is very large, we might not actually
care about what IDs were generated.

## Selecting records

## Counting records

## Handling errors

SqliteDatastore wraps underlying sqlite errors in its own error types:

| Class | Code | Description |
| -- | -- | -- |
| `InsertError` | `INSERT_ERROR` | An error occurred while inserting a record. |
| `InvalidSchemaError` | `INVALID_SCHEMA` | The schema provided to the datastore is invalid. |
| `NoSuchTableError` | `NO_SUCH_TABLE` | The table does not exist. |
| `UniqueConstraintViolationError` | `UNIQUE_CONSTRAINT_VIOLATION` | A unique constraint was violated. |
| `UnknownError` | `UNKNOWN_ERROR` | An unknown error occurred (see the error message for details). |

The base class for these errors is `SqliteDatastoreError`.
