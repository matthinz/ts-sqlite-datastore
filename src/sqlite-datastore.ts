import crypto from "node:crypto";
import sqlite3, { Database, Statement } from "sqlite3";

/*

# ts-sqlite-datastore

Version: 0.0.1

[![Node.js CI](https://github.com/matthinz/ts-sqlite-datastore/actions/workflows/node.js.yml/badge.svg)](https://github.com/matthinz/ts-sqlite-datastore/actions/workflows/node.js.yml)

This is a single file, Promise-oriented API for working with a local sqlite
database in Node.js.

It is a work in progress.

*/

/*

## Column types

sqlite represents data using four types:

- `TEXT`
- `BLOB`
- `INTEGER`
- `REAL`

*/

const SQLITE_TYPES = {
  TEXT: true,
  BLOB: true,
  INTEGER: true,
  REAL: true,
} as const;

/**
 * `SqliteNativeType` is one of the four native types supported by sqlite.
 */
export type SqliteNativeType = keyof typeof SQLITE_TYPES;
/*

These four map *somewhat* cleanly onto Javascript types.

Of course, values can also be `NULL`, which Javascript helpfully represents in
two ways: `null` and `undefined`.

The `JsTypeForSqliteNativeType` helper allows us to convert between sqlite types
and Javascript types, e.g.:

```ts
type T = JsTypeForSqliteNativeType<"TEXT", false>; // string
type NullableT = JsTypeForSqliteNativeType<"TEXT", true>; // string | null
```

*/

export type JsTypeForSqliteNativeType<
  T extends SqliteNativeType,
  Nullable extends boolean | undefined,
> = Nullable extends true
  ? T extends "TEXT"
    ? string | null
    : T extends "BLOB"
      ? Buffer | string | null
      : T extends "INTEGER"
        ? number | bigint | null
        : T extends "REAL"
          ? number | null
          : never
  : T extends "TEXT"
    ? string
    : T extends "BLOB"
      ? Buffer | string
      : T extends "INTEGER"
        ? number | bigint
        : T extends "REAL"
          ? number
          : never;

/*

### Custom types

For convenience, we also allow certain special "custom" types.

| Type | Description |
| -- | -- |
| `uuid` | A universally-unique identifier, stored as a `TEXT` column with `UNIQUE` and `NOT NULL` constraints by default. |
| `insert_timestamp` | A timestamp that is automatically set to the current time when a record is inserted. Stored as a `TEXT` column with a `NOT NULL` constraint. |
| `update_timestamp` | A timestamp that is automatically set to the current time when a record is updated. Stored as a `TEXT` column with a `NOT NULL` constraint. |

*/

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

type BeforeInsertHook = (
  record: Record<string, unknown>,
  columnName: string,
  columnSchema: ColumnSchema,
  tableName: string,
) => void;

type BeforeUpdateHook = (
  record: Record<string, unknown>,
  columnName: string,
  columnSchema: ColumnSchema,
  tableName: string,
) => void;

type CustomTypeDefinition<
  T extends SqliteNativeType,
  JsType,
  Nullable extends boolean,
  Unique extends boolean,
> = {
  beforeInsert?: BeforeInsertHook;
  beforeUpdate?: BeforeUpdateHook;
  type: T;
  nullable: Nullable;
  parse?(value: JsTypeForSqliteNativeType<T, false>): JsType;
  unique: Unique;
};

type CustomTypeMap = {
  [key: string]: CustomTypeDefinition<any, any, any, any>;
};

const CUSTOM_TYPES: CustomTypeMap = {
  uuid: {
    type: "TEXT",
    nullable: false,
    unique: true,
    beforeInsert: (record, columnName) => {
      if (record[columnName] == null) {
        record[columnName] = crypto.randomUUID();
        return;
      }

      if (!UUID_REGEX.test(String(record[columnName]))) {
        throw new InvalidUUIDError();
      }
    },
    beforeUpdate: (record, columnName) => {
      if (record[columnName] == null) {
        return;
      }

      if (!UUID_REGEX.test(String(record[columnName]))) {
        throw new InvalidUUIDError();
      }
    },
    parse: (value) => value,
  },
  insert_timestamp: {
    type: "TEXT",
    nullable: false,
    unique: false,
    parse: (value) => new Date(value as string),
    beforeInsert: (record, columnName: string) => {
      if (record[columnName] != null) {
        throw new InsertError(
          "Specifying a value for an insert_timestamp column is not allowed",
        );
      }
      record[columnName] = new Date().toISOString();
    },
    beforeUpdate: (record, columnName: string) => {
      if (record[columnName] != null) {
        throw new UpdateError(
          "Specifying a value for an insert_timestamp column is not allowed",
        );
      }
    },
  },
  update_timestamp: {
    type: "TEXT",
    nullable: false,
    unique: false,
    beforeInsert: (record, columnName: string) => {
      if (record[columnName] != null) {
        throw new InsertError(
          "Specifying a value for an update_timestamp column is not allowed",
        );
      }

      record[columnName] = new Date().toISOString();
    },
    beforeUpdate: (record, columnName: string) => {
      if (record[columnName] != null) {
        throw new UpdateError(
          "Specifying a value for an update_timestamp column is not allowed",
        );
      }

      record[columnName] = new Date().toISOString();
    },
    parse: (value) => {
      return new Date(value as string);
    },
  },
} as const;

type CustomTypeName = keyof typeof CUSTOM_TYPES;

type SqliteNativeTypeForCustomType<T extends CustomTypeName> =
  (typeof CUSTOM_TYPES)[T]["type"];

type JsTypeForCustomType<
  T extends CustomTypeName,
  Nullable extends boolean | undefined = undefined,
> =
  (typeof CUSTOM_TYPES)[T] extends CustomTypeDefinition<
    SqliteNativeType,
    infer JsType,
    infer DefaultNullable,
    any
  >
    ? Nullable extends boolean
      ? Nullable extends true
        ? JsType | null
        : JsType
      : DefaultNullable extends true
        ? JsType | null
        : JsType
    : never;

type JsTypeFor<T, Nullable = false> = Nullable extends true
  ? T extends CustomTypeName
    ? JsTypeForCustomType<T> | void
    : T extends SqliteNativeType
      ? JsTypeForSqliteNativeType<T, false> | void
      : never
  : T extends CustomTypeName
    ? JsTypeForCustomType<T>
    : T extends SqliteNativeType
      ? JsTypeForSqliteNativeType<T, false>
      : never;

type SqliteNativeTypeFor<
  T extends
    | SqliteNativeType
    | CustomTypeName
    | AutoIncrementableColumnSchema
    | NativeTypeColumnSchema<any, any, any, any, any>,
> = T extends SqliteNativeType
  ? T
  : T extends CustomTypeName
    ? SqliteNativeTypeForCustomType<T>
    : T extends AutoIncrementableColumnSchema
      ? "INTEGER"
      : T extends NativeTypeColumnSchema<infer NativeType, any, any, any, any>
        ? NativeType
        : T extends CustomTypeColumnSchema<infer CustomType, any, any>
          ? SqliteNativeTypeForCustomType<CustomType>
          : never;

/*

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

You can specify the primary key on your table as a column name or an array of column names:

```ts
{
  tables: {
    users: { /* ... * / }
    primaryKey: "id"
}
```

If not provided, the primary key defaults to `id`.

*/

type AutoIncrementableColumnSchema = {
  type: "INTEGER";

  /**
   * Whether this column's value should auto-increment.
   */
  autoIncrement: true;
};

type CustomTypeColumnSchema<
  T extends CustomTypeName,
  Nullable extends boolean | undefined = (typeof CUSTOM_TYPES)[T]["nullable"],
  Unique extends boolean | undefined = (typeof CUSTOM_TYPES)[T]["unique"],
> = {
  type: T;

  /**
   * Whether this column can contain NULL values.
   */
  nullable?: Nullable;

  /**
   * Whether values in this column must be unique.
   */
  unique?: Unique;
};

type NativeTypeColumnSchema<
  T extends SqliteNativeType,
  ParsedType,
  Nullable extends boolean = false,
  DefaultValue = JsTypeFor<T>,
  Unique extends boolean = false,
> = {
  type: T;

  /**
   * Value to insert as a default.
   */
  defaultValue?: DefaultValue;

  /**
   * Whether this column can contain NULL values.
   */
  nullable?: Nullable;

  /**
   * If provided, a function used to parse data from the database into a
   * native Javascript representation.
   */
  parse?: (value: JsTypeFor<T, Nullable>) => ParsedType;

  /**
   * If provided, a function used to serialize native Javascript values
   * back to a form used by the database.
   */
  serialize?: (value: unknown) => JsTypeFor<T, Nullable>;

  /**
   * Whether values in this column must be unique.
   * Defaults to `false`.
   */
  unique?: Unique;
};

export type ColumnSchema =
  | AutoIncrementableColumnSchema
  | NativeTypeColumnSchema<SqliteNativeType, any, any, any>
  | CustomTypeColumnSchema<CustomTypeName>;

export type JsTypeForColumnSchema<
  ColumnSchemaType extends
    | SqliteNativeType
    | CustomTypeName
    | NativeTypeColumnSchema<SqliteNativeType, any, any, any>
    | CustomTypeColumnSchema<CustomTypeName>,
> = ColumnSchemaType extends SqliteNativeType
  ? // We've been given a literal string for a sqlite type, e.g. "TEXT"
    JsTypeForSqliteNativeType<ColumnSchemaType, false>
  : ColumnSchemaType extends CustomTypeName
    ? // We've been given a custom type name, e.g. "uuid"
      JsTypeForCustomType<ColumnSchemaType>
    : ColumnSchemaType extends NativeTypeColumnSchema<
          infer T,
          infer ParsedType,
          infer Nullable,
          any
        >
      ? // We've been given a column schema object based on a native type, e.g. { "type": "TEXT" }
        ColumnSchemaType extends {
          type: T;
          parse: (input: JsTypeForSqliteNativeType<T, Nullable>) => ParsedType;
        }
        ? // A parse function was provided, so use its return type
          ParsedType
        : // No parse function was provided, so use the specified type
          Nullable extends true
          ? // Column is nullable
            JsTypeForSqliteNativeType<T, true>
          : // Column is not nullable
            JsTypeForSqliteNativeType<T, false>
      : ColumnSchemaType extends CustomTypeColumnSchema<
            infer T,
            infer Nullable,
            any
          >
        ? // We've been given a schema based on a custom type
          JsTypeForCustomType<T, Nullable>
        : // We've been given an invalid schema
          never;

export type TableSchema<ColumnNames extends string> = {
  columns: {
    [columnName in ColumnNames]:
      | AutoIncrementableColumnSchema
      | NativeTypeColumnSchema<SqliteNativeType, any, any, any>
      | CustomTypeColumnSchema<CustomTypeName, any, any>
      | CustomTypeName
      | SqliteNativeType;
  };
  primaryKey?: ColumnNames | ColumnNames[];
};

export type ColumnNames<Table extends TableSchema<string>> =
  keyof Table["columns"];

/**
 * Make some keys in T optional.
 */
type MakeOptional<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>;

type IsNullable<
  T extends
    | SqliteNativeType
    | CustomTypeName
    | AutoIncrementableColumnSchema
    | NativeTypeColumnSchema<any, any, any, any>
    | CustomTypeColumnSchema<any, any, any>,
> = T extends SqliteNativeType
  ? false
  : T extends CustomTypeName
    ? (typeof CUSTOM_TYPES)[T]["nullable"]
    : T extends AutoIncrementableColumnSchema
      ? false
      : T extends NativeTypeColumnSchema<infer T, any, infer Nullable, any>
        ? Nullable
        : T extends CustomTypeColumnSchema<infer T, infer Nullable, any>
          ? Nullable
          : never;

type MapWhereNullableColumnsHaveColumnNameAsValue<
  T extends TableSchema<string>,
> = {
  [columnName in keyof T["columns"]]: IsNullable<
    T["columns"][columnName]
  > extends true
    ? columnName
    : never;
};

/**
 * Extract only the nullable column names from a TableSchema.
 */
type NullableColumnNames<T extends TableSchema<string>> =
  MapWhereNullableColumnsHaveColumnNameAsValue<T>[keyof T["columns"]];

type MapWhereColumnsWithDefaultValueHaveColumnNameAsValue<
  T extends TableSchema<string>,
> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends {
    defaultValue: void;
  }
    ? never
    : T["columns"][columnName] extends { defaultValue: any }
      ? columnName
      : never;
};

/**
 * Extract only the names of columns that have default values set
 */
type NamesOfColumnsWithDefaultValues<T extends TableSchema<string>> =
  MapWhereColumnsWithDefaultValueHaveColumnNameAsValue<T>[keyof T["columns"]];

type PrimaryKeyColumnsNames<T extends TableSchema<string>> =
  T["primaryKey"] extends string
    ? T["primaryKey"]
    : T["primaryKey"] extends string[]
      ? T["primaryKey"][number]
      : never;

/*

To derive a Javascript type for a record in a table, you can use
`RecordFor<Table>`.

*/

/**
 * The "raw" record is what is actually returned from the database, before
 * we've done any parsing.
 */
export type RawRecordFor<Table extends TableSchema<string>> = {
  [columnName in keyof Table["columns"]]: IsNullable<
    Table["columns"][columnName]
  > extends true
    ? JsTypeFor<SqliteNativeTypeFor<Table["columns"][columnName]>, true>
    : JsTypeFor<SqliteNativeTypeFor<Table["columns"][columnName]>, false>;
};

export type RecordFor<Table extends TableSchema<string>> = {
  [columnName in keyof Table["columns"]]: Table["columns"][columnName] extends SqliteNativeType
    ? JsTypeForSqliteNativeType<Table["columns"][columnName], false>
    : JsTypeForColumnSchema<Table["columns"][columnName]>;
};

/**
 * When a column has a default value defined, it doesn't need to be specified
 * on insert. InsertRecordFor<T> returns a Record type for the given table, with
 * all columns that have default values marked as Optional.
 */
export type InsertRecordFor<T extends TableSchema<string>> = Omit<
  MakeOptional<
    RecordFor<T>,
    NullableColumnNames<T> | NamesOfColumnsWithDefaultValues<T>
  >,
  PrimaryKeyColumnsNames<T>
>;

export type Schema = {
  tables: {
    [tableName: string]: TableSchema<string>;
  };
};

/**
 * Returns the set of table names defined in a Schema.
 */
export type TableNames<TSchema extends Schema> = keyof TSchema["tables"];

/*

## Creating a SqliteDatastore

The `SqliteDatastore` constructor an options object with the following properties:

| Property | Type | Description |
| -- | -- | -- |
| `schema` | `Schema` | The schema for the database (required). |
| `filename` | `string` | The name of the sqlite database file to open. If not provided, an in-memory database will be used. |

*/

export type SqliteDatastoreOptions<TSchema extends Schema> = {
  schema: TSchema;

  /**
   * If provided, the name of the sqlite database file to open.
   * If not provided, an in-memory database will be used.
   */
  filename?: string;

  /**
   * If specified, turns on verbose mode for the sqlite3 library.
   */
  verbose?: boolean;
};

/*

## Inserting records

We want to be able to cover a few different insertion scenarios:

1. Insert a single record
2. Insert multiple records

After we insert records, it should be possible to:

- Get the number of records inserted
- Get the IDs of records that were auto-incremented

However, if the number of records inserted is very large, we might not actually
care about what IDs were generated.

*/

/**
 * The value returned by an insert operation, including the set of
 * auto-incremented IDs that were generated.
 */
type InsertResultWithIds = {
  /**
   * The number of records inserted.
   */
  readonly count: number;

  /**
   * For Tables with an auto-incrementing ID column, the IDs that were
   * generated. IDs will be returned in the same order that records
   * were provided.
   */
  readonly ids: number[];
};

type InsertResultWithoutIds = {
  /**
   * The number of records inserted.
   */
  readonly count: number;
};

// Build a map of column names to whether they auto-increment
type AutoIncrementingColumnMap<T extends TableSchema<string>> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends {
    autoIncrement: true;
  }
    ? // Column auto-increments
      true
    : // Column does not auto-increment
      false;
};

// Return whether a given table has an auto-incrementing column
type HasAutoIncrementingColumn<T extends TableSchema<string>> =
  AutoIncrementingColumnMap<T>[keyof T["columns"]] extends false
    ? // false means no column auto-increments
      false
    : AutoIncrementingColumnMap<T>[keyof T["columns"]] extends boolean
      ? // boolean means at least one column auto-increments
        true
      : false;

export type InsertResult<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
  Options extends InsertOptions<TSchema, TableName> | undefined = undefined,
> =
  HasAutoIncrementingColumn<TSchema["tables"][TableName]> extends true
    ? Options extends { returnIds: true }
      ? InsertResultWithIds
      : Options extends undefined
        ? InsertResultWithIds
        : InsertResultWithoutIds
    : InsertResultWithoutIds;

export type InsertOptions<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
> = {
  records: InsertRecordFor<TSchema["tables"][TableName]>[];
  returnIds?: boolean;
  table: TableName;
};

/*

## Selecting records

*/

export type SelectOptions<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
> = {
  table: TableName;
  where: Criteria<TSchema["tables"][TableName]>;
};

export type EqualToComparison<Table extends TableSchema<string>> = {
  eq: Table["columns"] | string | number | bigint;
};

export type NotEqualToComparison<Table extends TableSchema<string>> = {
  neq: Table["columns"] | string | number | bigint;
};

export type GreaterThanComparison<Table extends TableSchema<string>> = {
  gt: Table["columns"] | number | bigint;
};

export type GreaterThanOrEqualToComparison<Table extends TableSchema<string>> =
  {
    gte: Table["columns"] | number | bigint;
  };

export type LessThanComparison<Table extends TableSchema<string>> = {
  lt: Table["columns"] | number | bigint;
};

export type LessThanOrEqualToComparison<Table extends TableSchema<string>> = {
  eq: Table["columns"] | number | bigint;
};

export type LikeComparison<Table extends TableSchema<string>> = {
  like: Table["columns"] | string;
};

type CriteriaValuesForNumbers<Table extends TableSchema<string>> =
  | number
  | number[]
  | bigint
  | bigint[]
  | (number | bigint)[]
  | undefined
  | null
  | EqualToComparison<Table>
  | NotEqualToComparison<Table>
  | LikeComparison<Table>
  | GreaterThanComparison<Table>
  | GreaterThanOrEqualToComparison<Table>
  | LessThanComparison<Table>
  | LessThanOrEqualToComparison<Table>;

// Track what kinds of values we accept for a column-based criteria
export type ValueForCriteria<
  Table extends TableSchema<string>,
  ColumnName extends ColumnNames<Table>,
> =
  JsTypeForColumnSchema<Table["columns"][ColumnName]> extends
    | string
    | null
    | undefined
    ? /* string */ string | string[] | undefined | null
    : JsTypeForColumnSchema<Table["columns"][ColumnName]> extends
          | number
          | undefined
          | null
      ? /* number */
        CriteriaValuesForNumbers<Table>
      : JsTypeForColumnSchema<Table["columns"][ColumnName]> extends
            | number
            | bigint
            | undefined
            | null
        ? CriteriaValuesForNumbers<Table>
        : never;

export type Criteria<Table extends TableSchema<string>> = {
  [columnName in ColumnNames<Table>]?: ValueForCriteria<Table, columnName>;
};

/*

## Counting records

*/

export type CountOptions<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
> = {
  table: TableName;
  where?: Criteria<TSchema["tables"][TableName]>;
};

/*

## Updating records

We support batch updating records based on a WHERE clause.

```ts
await dataStore.update("people", {
  set: {
    birthdate: "1999-01-01",
  }
  where: { birthdate: null }
);
```

*/

type UpdateResult = {
  count: number;
};

export type UpdateOptions<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
> = {
  table: TableName;
  set: Partial<RecordFor<TSchema["tables"][TableName]>>;
  where?: Criteria<TSchema["tables"][TableName]>;
};

/*

## Deleting records

To delete records, you can use the `delete` method.

To delete all records in a table, you must specify `{ all: true }`:

```ts
await dataStore.delete("people", { all: true });
```

To delete only some records, you can specify a `where` clause:

```ts
await dataStore.delete("people", {
  where: {
    name: "Joey Joe-Joe Junior Shabadoo",
  },
});
```

`delete` returns a structure describing how the delete went:

```ts
const result = await dataStore.delete("people", { all: true });
console.log("%d record(s) were deleted", result.count);
```

*/

export type DeleteResult = {
  readonly count: number;
};

export type DeleteOptions<
  TSchema extends Schema,
  TableName extends TableNames<TSchema>,
> = {
  table: TableName;
} & (
  | { where: Criteria<TSchema["tables"][TableName]> }
  | {
      all: true;
    }
);

/*

## Handling errors

SqliteDatastore wraps underlying sqlite errors in its own error types:

| Class | Code | Description |
| -- | -- | -- |
| `InsertError` | `INSERT_ERROR` | An error occurred while inserting a record. |
| `InvalidSchemaError` | `INVALID_SCHEMA` | The schema provided to the datastore is invalid. |
| `NoSuchTableError` | `NO_SUCH_TABLE` | The table does not exist. |
| `SerializationError` | `SERIALIZATION_ERROR` | An error occurred while serializing a value for writing to the database. |
| `SyntaxError` | `SYNTAX_ERROR` | A syntax error occurred. |
| `UniqueConstraintViolationError` | `UNIQUE_CONSTRAINT_VIOLATION` | A unique constraint was violated. |
| `UnknownError` | `UNKNOWN_ERROR` | An unknown error occurred (see the error message for details). |
| `UpdateError` | `UPDATE_ERROR` | An error occurred while updating records. |

The base class for these errors is `SqliteDatastoreError`.

*/

const ERROR_CODES = [
  "INSERT_ERROR",
  "INVALID_SCHEMA",
  "INVALID_UUID",
  "NO_SUCH_TABLE",
  "SERIALIZATION_ERROR",
  "SYNTAX_ERROR",
  "UNIQUE_CONSTRAINT_VIOLATION",
  "UNKNOWN_ERROR",
  "UPDATE_ERROR",
] as const;

export type ErrorCode = (typeof ERROR_CODES)[number];

/**
 * This is the base Error class for all SqliteDatastore errors.
 */
export abstract class SqliteDatastoreError extends Error {
  readonly #code: string;

  constructor(message: string, code: ErrorCode) {
    super(message);
    this.name = this.constructor.name;
    this.#code = code;
  }

  get code(): string {
    return this.#code;
  }
}

export class InsertError extends SqliteDatastoreError {
  constructor(message: string) {
    super(message, "INSERT_ERROR");
  }
}

export class InvalidSchemaError extends SqliteDatastoreError {
  constructor(message: string) {
    super(message, "INVALID_SCHEMA");
  }
}

export class InvalidUUIDError extends SqliteDatastoreError {
  constructor() {
    super("The value provided is not a valid UUID", "INVALID_UUID");
  }
}

export class NoSuchTableError extends SqliteDatastoreError {
  constructor(public readonly tableName: string) {
    super(`No such table: ${tableName}`, "NO_SUCH_TABLE");
  }
}

export class SerializationError extends SqliteDatastoreError {
  #value: unknown;

  constructor(tableName: string, columnName: string, value: unknown) {
    super(
      `Failed to serialize value for "${tableName}"."${columnName}"`,
      "SERIALIZATION_ERROR",
    );

    this.#value = value;
  }

  get value(): unknown {
    return this.#value;
  }
}

export class SyntaxError extends SqliteDatastoreError {
  constructor(
    message: string,
    public readonly sql?: string,
  ) {
    if (sql != null) {
      message = `${message} (SQL: ${sql})`;
    }
    super(message, "SYNTAX_ERROR");
  }
}

export class UniqueConstraintViolationError extends SqliteDatastoreError {
  constructor(
    public readonly tableName: string,
    public readonly columnName: string,
  ) {
    super(
      `UNIQUE constraint violation: "${tableName}"."${columnName}"`,
      "UNIQUE_CONSTRAINT_VIOLATION",
    );
  }
}

export class UnknownError extends SqliteDatastoreError {
  constructor(message: string) {
    super(message, "UNKNOWN_ERROR");
  }
}

export class UpdateError extends SqliteDatastoreError {
  constructor(message: string) {
    super(message, "UPDATE_ERROR");
  }
}

export class SqliteDatastore<TSchema extends Schema> {
  readonly #filename: string;
  readonly #schema: TSchema;
  readonly #databasePromise: Promise<Database>;
  #migrated: boolean = false;

  constructor({
    schema,
    filename,
    verbose,
    ...rest
  }: SqliteDatastoreOptions<TSchema>) {
    this.#filename = filename ?? ":memory:";
    this.#schema = schema;
    this.#databasePromise = new Promise((resolve, reject) => {
      const Database = verbose ? sqlite3.verbose().Database : sqlite3.Database;
      const db = new Database(this.#filename, (err) => {
        const onDatabaseReady = (err: Error | null, db?: Database) => {
          if (
            "onDatabaseReady" in rest &&
            typeof rest.onDatabaseReady === "function"
          ) {
            rest.onDatabaseReady(err, db);
          }
        };

        if (err) {
          setImmediate(onDatabaseReady, err);
          reject(err);
          return;
        }

        setImmediate(onDatabaseReady, null, db);
        resolve(db);
      });
    });
  }

  /**
   * The filename of the database file being used.
   * For in-memory databases, this will be ':memory:'.
   * @returns {string}
   */
  get filename(): string {
    return this.#filename;
  }

  /**
   * Closes the database. Once closed, no further operations can
   * be performed on this instance of SqliteDatastore.
   * @returns {Promise<void>}
   */
  async close(): Promise<void> {
    return this.#databasePromise.then(
      (db) =>
        new Promise((resolve, reject) => {
          db.close((err) => {
            if (err) {
              reject(err);
              return;
            }
            resolve();
          });
        }),
    );
  }

  count<TableName extends TableNames<TSchema>>(
    tableName: TableName,
  ): Promise<number>;
  count<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    options: Omit<CountOptions<TSchema, TableName>, "table">,
  ): Promise<number>;
  count<TableName extends TableNames<TSchema>>(
    options: CountOptions<TSchema, TableName>,
  ): Promise<number>;
  count<TableName extends TableNames<TSchema>>(
    tableNameOrOptions: CountOptions<TSchema, TableName> | TableName,
    mayBeOptions?: Omit<CountOptions<TSchema, TableName>, "table">,
  ): Promise<number> {
    type O = SelectOptions<TSchema, TableName>;

    const options =
      typeof tableNameOrOptions === "string"
        ? mayBeOptions
          ? ({ ...mayBeOptions, table: tableNameOrOptions } as O)
          : ({ table: tableNameOrOptions } as O)
        : (tableNameOrOptions as O);

    const tableName = String(options.table);

    const sql = [`SELECT COUNT(*) FROM "${tableName}"`];
    const params: unknown[] = [];

    const [whereClause, whereParams] = this.buildWhereClause(options.where);

    if (whereClause) {
      sql.push(whereClause);
      params.push(...whereParams);
    }

    return this.migrateIfNeeded()
      .then(() => this.prepare(sql.join(" "), params))
      .then(
        (statement) =>
          new Promise<number>((resolve, reject) => {
            statement.get((err, row) => {
              if (err) {
                statement.finalize(() => reject(err));
                return;
              }

              statement.finalize((err) => {
                if (err) {
                  reject(err);
                  return;
                }
                resolve(Number((row as any)["COUNT(*)"]));
              });
            });
          }),
      );
  }

  delete<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    options: Omit<DeleteOptions<TSchema, TableName>, "table">,
  ): Promise<DeleteResult>;
  delete<TableName extends TableNames<TSchema>>(
    tableNameOrOptions: TableName | DeleteOptions<TSchema, TableName>,
    mayBeOptions: Omit<DeleteOptions<TSchema, TableName>, "table">,
  ): Promise<DeleteResult> {
    const options =
      typeof tableNameOrOptions === "string"
        ? { ...mayBeOptions, table: tableNameOrOptions }
        : (tableNameOrOptions as DeleteOptions<TSchema, TableName>);

    const tableName = String(options.table);
    const sql = [`DELETE FROM "${tableName}"`];
    const params: unknown[] = [];

    const [whereClause, whereParams] =
      "where" in options
        ? this.buildWhereClause(options.where)
        : [undefined, []];
    if (whereClause) {
      sql.push(whereClause);
      params.push(...whereParams);
    }

    return this.migrateIfNeeded().then(
      (db) =>
        new Promise<DeleteResult>((resolve, reject) => {
          db.run(sql.join(" "), params, function (err) {
            if (err) {
              reject(err);
              return;
            }

            resolve({ count: this.changes });
          });
        }),
    );
  }

  /**
   * Inserts a record into the given table.
   * @param tableName Table to insert into.
   * @param record The record to insert.
   * @returns {Promise<InsertResult>} A structure describing how the insert went.
   */
  async insert<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    record: InsertRecordFor<TSchema["tables"][TableName]>,
  ): Promise<InsertResult<TSchema, TableName>>;
  /**
   * Inserts multiple records into the given table.
   * @param tableName Table to insert into.
   * @param records The records to insert.
   * @returns {Promise<InsertResult>} A structure describing how the insert went.
   */
  async insert<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    records: InsertRecordFor<TSchema["tables"][TableName]>[],
  ): Promise<InsertResult<TSchema, TableName>>;
  /**
   * Inserts record(s) into a table, with a number of other options exposed.
   * @param options Structure describing the insert operation.
   * @returns {Promise<InsertResult>} A structure describing how the insert went.
   */
  async insert<TableName extends TableNames<TSchema>>(
    options: InsertOptions<TSchema, TableName>,
  ): Promise<
    InsertResult<TSchema, TableName, InsertOptions<TSchema, TableName>>
  >;
  async insert<TableName extends TableNames<TSchema>>(
    tableNameOrInsertOptions: TableName | InsertOptions<TSchema, TableName>,
    recordOrRecords?:
      | InsertRecordFor<TSchema["tables"][TableName]>
      | InsertRecordFor<TSchema["tables"][TableName]>[],
  ) {
    await this.migrateIfNeeded();

    const options: InsertOptions<TSchema, TableName> =
      typeof tableNameOrInsertOptions === "string"
        ? {
            records: Array.isArray(recordOrRecords)
              ? recordOrRecords
              : [recordOrRecords!],
            table: tableNameOrInsertOptions,
          }
        : (tableNameOrInsertOptions as InsertOptions<TSchema, TableName>);

    const { records } = options;
    const tableName = String(options.table);
    const tableSchema = this.#schema["tables"][
      tableName
    ] as TSchema["tables"][TableName];

    const { records: recordsForInsert, columnNames } = this.getRecordsForInsert(
      tableName,
      tableSchema,
      records,
    );

    const sql = [
      `INSERT INTO "${tableName}" `,
      "(",
      columnNames.map((c) => `"${c}"`).join(","),
      ") VALUES (",
      columnNames.map(() => "?").join(","),
      ")",
    ].join("");

    // TODO: Multi-insert in batches

    const statement = await this.prepare(sql);

    return recordsForInsert
      .reduce<Promise<InsertResult<TSchema, TableName>>>(
        (promise, record) =>
          promise.then(async (result) => {
            const params = columnNames.map((columnName) => record[columnName]);

            const { lastID } = await this.runStatement(statement, params);
            (result as any).count += 1;

            if (lastID > 0 && "ids" in result) {
              result.ids.push(lastID);
            }

            return result;
          }),
        Promise.resolve({
          count: 0,
          ids: [],
        }),
      )
      .then(
        (result) =>
          new Promise((resolve, reject) => {
            statement.finalize((err) => {
              if (err) {
                reject(err);
                return;
              }
              resolve(result);
            });
          }),
        (err) =>
          new Promise((_resolve, reject) => {
            statement.finalize(() => {
              reject(this.adaptSqliteError(err, sql));
            });
          }),
      );
  }

  migrate(): Promise<void> {
    return this.migrateIfNeeded().then(() => {});
  }

  select<TableName extends TableNames<TSchema>>(
    tableName: TableName,
  ): Promise<RecordFor<TSchema["tables"][TableName]>[]>;
  select<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    options: Omit<SelectOptions<TSchema, TableName>, "table">,
  ): Promise<RecordFor<TSchema["tables"][TableName]>[]>;
  select<TableName extends TableNames<TSchema>>(
    options: SelectOptions<TSchema, TableName>,
  ): Promise<RecordFor<TSchema["tables"][TableName]>[]>;
  select<TableName extends TableNames<TSchema>>(
    tableNameOrOptions: TableName | SelectOptions<TSchema, TableName>,
    mayBeOptions?: Omit<SelectOptions<TSchema, TableName>, "table">,
  ): Promise<RecordFor<TSchema["tables"][TableName]>[]> {
    type O = SelectOptions<TSchema, TableName>;

    const options =
      typeof tableNameOrOptions === "string"
        ? mayBeOptions
          ? ({ ...mayBeOptions, table: tableNameOrOptions } as O)
          : ({ table: tableNameOrOptions } as O)
        : (tableNameOrOptions as O);

    const tableName = String(options.table);
    const tableSchema = this.#schema["tables"][
      tableName
    ] as TSchema["tables"][TableName];

    const sqlClauses = [`SELECT * FROM "${tableName}"`];
    const params: unknown[] = [];

    const [whereClause, whereParams] = this.buildWhereClause(options.where);

    if (whereClause) {
      sqlClauses.push(whereClause);
      params.push(...whereParams);
    }

    const sql = sqlClauses.join(" ");

    type ResultRecord = RecordFor<TSchema["tables"][TableName]>;

    return Promise.all([this.migrateIfNeeded(), this.prepare(sql, params)])
      .then(
        ([_, statement]) =>
          new Promise<ResultRecord[]>((resolve, reject) => {
            const rows = [] as ResultRecord[];
            statement.each(
              (err, row) => {
                if (err) {
                  statement.finalize(() => reject(err));
                  return;
                }
                rows.push(
                  this.parseRow(
                    tableSchema,
                    row as RawRecordFor<TSchema["tables"][TableName]>,
                  ),
                );
              },
              (err) => {
                statement.finalize((_finalizeErr) => {
                  if (err) {
                    reject(err);
                    return;
                  }
                  resolve(rows);
                });
              },
            );
          }),
      )
      .catch((err) => Promise.reject(this.adaptSqliteError(err, sql)));
  }

  protected adaptSqliteError(err: Error, sql?: string): Error {
    if (!("errno" in err)) {
      return err;
    }

    let m: RegExpExecArray | null;

    switch (err.errno) {
      case 1:
        m = /SQLITE_ERROR: no such table: (.*)/.exec(err.message);
        if (m) {
          return new NoSuchTableError(m[1]);
        }

        m = /SQLITE_ERROR: near .*: syntax error/.exec(err.message);
        if (m) {
          return new SyntaxError(err.message, sql);
        }

        return new UnknownError(err.message);

      case 19:
        m = /UNIQUE constraint failed: (.*)\.(.*)/.exec(err.message);
        if (m) {
          return new UniqueConstraintViolationError(m[1], m[2]);
        }
        break;
    }

    return err;
  }

  update<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    records: RecordFor<TSchema["tables"][TableName]>[],
  ): Promise<UpdateResult>;
  update<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    options: Omit<UpdateOptions<TSchema, TableName>, "table">,
  ): Promise<UpdateResult>;
  update<TableName extends TableNames<TSchema>>(
    options: UpdateOptions<TSchema, TableName>,
  ): Promise<UpdateResult>;
  update<TableName extends TableNames<TSchema>>(
    optionsOrTableName: UpdateOptions<TSchema, TableName> | TableName,
    maybeOptionsOrRecords?:
      | Omit<UpdateOptions<TSchema, TableName>, "table">
      | RecordFor<TSchema["tables"][TableName]>[],
  ): Promise<UpdateResult> {
    return this.migrateIfNeeded().then(async (db) => {
      const options =
        typeof optionsOrTableName === "string"
          ? Array.isArray(maybeOptionsOrRecords)
            ? { records: maybeOptionsOrRecords, table: optionsOrTableName }
            : { ...maybeOptionsOrRecords, table: optionsOrTableName }
          : (optionsOrTableName as UpdateOptions<TSchema, TableName>);

      const tableName = String(options.table) as TableName;

      const tableSchema = this.#schema["tables"][
        tableName as string
      ] as TSchema["tables"][TableName];

      if ("records" in options) {
        throw new Error(
          "Support for updating specific records directly is not yet implemented.",
        );
      }

      const set = "set" in options ? options.set : undefined;

      if (!set) {
        throw new Error();
      }

      const valuesForUpdate = this.getValuesForUpdate(
        tableName,
        tableSchema,
        set,
      );

      const sqlClauses = [`UPDATE "${String(tableName)}" SET `];
      const params: unknown[] = [];

      Object.entries(valuesForUpdate).forEach(([columnName, value]) => {
        if (sqlClauses.length > 1) {
          sqlClauses.push(", ");
        }
        sqlClauses.push(`"${columnName}" = ?`);
        params.push(value);
      });

      const [whereClause, whereParams] =
        "where" in options
          ? this.buildWhereClause(options.where)
          : [undefined, []];

      if (whereClause) {
        sqlClauses.push(whereClause);
        params.push(...whereParams);
      }

      const sql = sqlClauses.join("");

      return new Promise<UpdateResult>((resolve, reject) => {
        db.run(sql, params, function (err) {
          if (err) {
            reject(err);
            return;
          }

          resolve({ count: this.changes });
        });
      }).catch((err) => Promise.reject(this.adaptSqliteError(err, sql)));
    });
  }

  protected buildWhereClause<Table extends TableSchema<string>>(
    where: Criteria<Table> | undefined,
  ): [string, unknown[]] {
    const [criteriaSql, params] = buildCriteriaSql(where);

    if (criteriaSql.length > 0) {
      return [`WHERE ${criteriaSql}`, params];
    }

    return ["", []];

    function buildCriteriaSql(
      criteria: Criteria<Table> | undefined,
    ): [string, unknown[]] {
      if (!criteria) {
        return ["", []];
      }

      const OPERATOR_MAP = {
        eq: "=",
        neq: "!=",
        gt: ">",
        gte: ">=",
        lt: "<",
        lte: "<=",
        like: "LIKE",
      };

      const [sql, params] = Object.entries(criteria).reduce<
        [string[], unknown[]]
      >(
        ([sql, params], [columnName, value]) => {
          if (Array.isArray(value)) {
            const placeholders: string[] = [];
            value.forEach((v) => {
              placeholders.push("?");
              params.push(v);
            });
            sql.push(`("${columnName}" IN (${placeholders.join(",")}))`);
          } else if (value == null) {
            sql.push(`("${columnName}" IS NULL)`);
          } else if (typeof value === "object") {
            Object.entries(OPERATOR_MAP).forEach(([operator, sqlOperator]) => {
              if (operator in value) {
                sql.push(`("${columnName}" ${sqlOperator} ?)`);
                params.push(value[operator]);
              }
            });
          } else {
            sql.push(`("${columnName}" = ?)`);
            params.push(value);
          }

          return [sql, params];
        },
        [[], []],
      );

      return [sql.join(" AND "), params];
    }
  }

  protected createTable<Table extends TableSchema<string>>(
    tableName: string,
    tableSchema: TableSchema<string>,
  ): Promise<void> {
    const columnNames: ColumnNames<Table>[] = Object.keys(tableSchema.columns);

    const columnDefinitions = columnNames.map((columnName) => {
      const columnNameAsString = String(columnName);

      const columnSchema = this.resolveColumnSchema(
        tableSchema.columns[columnNameAsString]!,
      );

      const { type } = columnSchema;

      if (!this.isValidColumnType(type)) {
        throw new InvalidSchemaError(
          `Invalid type '${type}' for column '${columnNameAsString}' in table '${tableName}'`,
        );
      }

      const isPrimaryKey = Array.isArray(tableSchema.primaryKey)
        ? tableSchema.primaryKey.includes(String(columnName))
        : tableSchema.primaryKey === columnName ||
          (String(columnName) === "id" && !("primaryKey" in tableSchema));

      const autoIncrement =
        "autoIncrement" in columnSchema && columnSchema.autoIncrement;
      const nullable = "nullable" in columnSchema && !!columnSchema.nullable;
      const unique = "unique" in columnSchema && !!columnSchema.unique;

      if (autoIncrement && !isPrimaryKey) {
        throw new InvalidSchemaError(
          `Column '${columnNameAsString}' in table '${tableName}' is marked as auto-incrementing but is not part of the table's primary key.`,
        );
      }

      const sql = [
        `"${columnNameAsString}"`,
        type,
        isPrimaryKey && "PRIMARY KEY",
        autoIncrement && "AUTOINCREMENT",
        nullable ? "NULL" : "NOT NULL",
        (autoIncrement || unique) && "UNIQUE",
      ]
        .filter(Boolean)
        .join(" ");

      return sql;
    });

    const sql = [
      `CREATE TABLE IF NOT EXISTS "${tableName}" (`,
      columnDefinitions.join(", "),
      ")",
    ].join("");

    return this.executeSql(sql);
  }

  /**
   * Default beforeInsert hook implementation.
   * Adds support for custom column serializers.
   * @param record
   * @param tableName
   * @param columnName
   * @param columnSchema
   */
  defaultBeforeInsert(
    record: Record<string, unknown>,
    columnName: string,
    columnSchema: ColumnSchema,
    _tableName: string,
  ) {
    let value = record[columnName];

    if (value == null) {
      if (typeof columnSchema === "object" && "defaultValue" in columnSchema) {
        value = record[columnName] = columnSchema.defaultValue;
      }
    }

    const serialize =
      typeof columnSchema === "object" &&
      "serialize" in columnSchema &&
      typeof columnSchema.serialize === "function"
        ? columnSchema.serialize
        : undefined;

    if (serialize != null) {
      record[columnName] = serialize(value);
    }
  }

  /**
   * Default beforeUpdate hook implementation.
   * Adds support for custom column serializers.
   * @param record
   * @param tableName
   * @param columnName
   * @param columnSchema
   */
  defaultBeforeUpdate(
    record: Record<string, unknown>,
    columnName: string,
    columnSchema: ColumnSchema,
    _tableName: string,
  ) {
    const value = record[columnName];

    const serialize =
      typeof columnSchema === "object" &&
      "serialize" in columnSchema &&
      typeof columnSchema.serialize === "function"
        ? columnSchema.serialize
        : undefined;

    if (serialize != null) {
      record[columnName] = serialize(value);
    }
  }

  protected executeSql(sql: string, ...params: unknown[]): Promise<void> {
    return this.prepare(sql).then((statement) =>
      new Promise<void>((resolve, reject) => {
        statement.run(params, function (err) {
          if (err) {
            reject(err);
            return;
          }
          statement.finalize((err) => {
            if (err) {
              reject(err);
              return;
            }
            resolve();
          });
        });
      }).catch((err) => {
        return Promise.reject(err);
      }),
    );
  }

  protected migrateIfNeeded(): Promise<Database> {
    return this.#databasePromise.then(async (db) => {
      if (this.#migrated) {
        return db;
      }

      this.#migrated = true;

      // Create tables
      await Object.entries(this.#schema.tables).reduce<Promise<void>>(
        (promise, [tableName, tableSchema]) =>
          promise.then(() => this.createTable(tableName, tableSchema)),
        Promise.resolve(),
      );

      return db;
    });
  }

  protected parseRow<Table extends TableSchema<string>>(
    tableSchema: Table,
    row: RawRecordFor<Table>,
  ): RecordFor<Table> {
    const parsedRow = {} as RecordFor<Table>;

    for (const [columnName, columnValue] of Object.entries(row)) {
      const rawColumnSchema =
        tableSchema.columns[columnName as keyof typeof tableSchema.columns];

      const columnSchema =
        typeof rawColumnSchema === "string" && rawColumnSchema in CUSTOM_TYPES
          ? CUSTOM_TYPES[rawColumnSchema]
          : rawColumnSchema;

      if (
        typeof columnSchema === "object" &&
        "parse" in columnSchema &&
        typeof columnSchema.parse === "function"
      ) {
        parsedRow[columnName as keyof typeof parsedRow] =
          columnSchema.parse(columnValue);
      } else {
        parsedRow[columnName as keyof typeof parsedRow] = columnValue;
      }
    }

    return parsedRow;
  }

  protected prepare(sql: string, params?: unknown[]): Promise<Statement> {
    const _self = this;
    return this.#databasePromise.then(
      (db) =>
        new Promise((resolve, reject) => {
          db.prepare(sql, params, function (err) {
            if (err) {
              reject(_self.adaptSqliteError(err, sql));
              return;
            }
            resolve(this);
          });
        }),
    );
  }

  /**
   * Given an Array of records being inserted, returns a _new_ Array where each
   * member has all columns that will be inserted, with beforeInsert hooks
   * applied.
   * @param tableName
   * @param tableSchema
   * @param records
   */
  private getRecordsForInsert<Table extends TableSchema<string>>(
    tableName: string,
    tableSchema: Table,
    records: InsertRecordFor<Table>[],
  ): { records: Record<string, unknown>[]; columnNames: string[] } {
    const columnNames = new Set<string>(Object.keys(tableSchema.columns));

    type BeforeInsertHook = (r: Record<string, unknown>) => void;
    const beforeInsertHooks: BeforeInsertHook[] = [];

    for (const columnName of columnNames) {
      const columnSchema = this.resolveColumnSchema(
        tableSchema.columns[columnName],
      );

      const beforeInsert = this.resolveBeforeInsertHook(columnSchema);

      if (beforeInsert != null) {
        beforeInsertHooks.push((r) => {
          beforeInsert!(r, columnName, columnSchema, tableName);
        });
      }
    }

    const recordsForInsert = records.map((record) => {
      // Allocate a new object to hold the new record's values.
      // There may be scenarios where we can avoid allocation, and
      // we may eventually want to allow opting into (or out of)
      // in-place modification.
      // We're also checking for column names that aren't actually in the
      // schema--we could make that opt-in/out as well.

      // TODO: Use Set.prototype.difference when we are targeting >= Node 22
      for (const key of Object.keys(record)) {
        if (!columnNames.has(key)) {
          throw new InsertError(
            `Column '${key}' not found on table '${tableName}'`,
          );
        }
      }

      const newRecord: Record<string, unknown> = {};

      for (const columnName of columnNames) {
        newRecord[columnName] =
          record[columnName as keyof InsertRecordFor<Table>];
      }

      // Fire the `beforeInsert` hook for each column.
      // These will handle things like custom serialization and default values.
      beforeInsertHooks.forEach((hook) => hook(newRecord));

      return newRecord;
    });

    return {
      records: recordsForInsert,
      columnNames: Array.from(columnNames),
    };
  }

  /**
   * Given a set of values for an update, resolves the set of values to
   * _actually_ be applied (after running serialization etc.)
   * @param tableName
   * @param tableSchema
   * @param set
   */
  private getValuesForUpdate<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    tableSchema: TSchema["tables"][TableName],
    set: Partial<RecordFor<TSchema["tables"][TableName]>>,
  ): Record<string, unknown> {
    const values: Record<string, unknown> = {};
    const columnNamesInUpdate = Object.keys(set);

    columnNamesInUpdate.forEach((columnName) => {
      if (tableSchema.columns[columnName] == null) {
        throw new UpdateError(`Invalid column name: "${columnName}"`);
      }
      values[columnName] = set[columnName];
    });

    // Apply beforeUpdate hooks
    columnNamesInUpdate.forEach((columnName) => {
      const columnSchema = this.resolveColumnSchema(
        tableSchema.columns[columnName],
      );

      if ("beforeUpdate" in columnSchema) {
        if (typeof columnSchema.beforeUpdate !== "function") {
          throw new InvalidSchemaError(`Invalid beforeUpdate hook`);
        }

        const beforeUpdate = columnSchema.beforeUpdate as BeforeUpdateHook;
        beforeUpdate(values, columnName, columnSchema, String(tableName));

        return;
      }

      const hasCustomSerialization = "serialize" in columnSchema;

      if (hasCustomSerialization) {
        this.defaultBeforeUpdate(
          values,
          columnName,
          columnSchema,
          String(tableName),
        );
      }
    });

    return values;
  }

  private isValidColumnType(type: unknown): boolean {
    if (typeof type !== "string") {
      return false;
    }

    return type?.toUpperCase() in SQLITE_TYPES || type in CUSTOM_TYPES;
  }

  private resolveBeforeInsertHook(
    columnSchema: ColumnSchema | CustomTypeName | SqliteNativeType,
  ): BeforeInsertHook | undefined {
    const resolvedColumnSchema = this.resolveColumnSchema(columnSchema);

    if (typeof resolvedColumnSchema === "object") {
      if ("beforeInsert" in resolvedColumnSchema) {
        if (typeof resolvedColumnSchema.beforeInsert !== "function") {
          throw new InvalidSchemaError(`Invalid beforeInsert hook`);
        }

        return resolvedColumnSchema.beforeInsert as BeforeInsertHook;
      }
    }

    // If no beforeInsert hook is defined, we fall back to
    // defaultBeforeInsertHook. However, if there are no features of this
    // column that would _require_ a beforeInsert hook, we can skip it.

    const hasDefaultValue =
      "defaultValue" in resolvedColumnSchema &&
      resolvedColumnSchema.defaultValue != null;
    const hasCustomSerialization =
      typeof resolvedColumnSchema === "object" &&
      "serialize" in resolvedColumnSchema;

    if (hasDefaultValue || hasCustomSerialization) {
      return this.defaultBeforeInsert;
    }
  }

  private resolveColumnSchema(
    columnSchema: ColumnSchema | CustomTypeName | SqliteNativeType,
  ): ColumnSchema {
    if (typeof columnSchema === "string") {
      if (columnSchema in CUSTOM_TYPES) {
        return CUSTOM_TYPES[columnSchema];
      } else if (columnSchema in SQLITE_TYPES) {
        return { type: columnSchema as SqliteNativeType };
      }
    } else if (typeof columnSchema === "object") {
      return columnSchema;
    }

    // TODO: Better error with table + column names
    throw new InvalidSchemaError(`Invalid column type: ${columnSchema}`);
  }

  private runStatement(
    statement: Statement,
    params?: unknown[],
  ): Promise<{ lastID: number; changes: number }> {
    return new Promise((resolve, reject) => {
      statement.run(params, function (err) {
        if (err) {
          reject(err);
          return;
        }
        resolve({
          lastID: this.lastID,
          changes: this.changes,
        });
      });
    });
  }
}
