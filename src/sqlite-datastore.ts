import { Database, Statement } from "sqlite3";

/*

# ts-sqlite-datastore

Version: 0.0.1

This is a single file, Promise-oriented API for working with a local sqlite
database in Node.js.

*/

/*

## Column types

sqlite represents data using four types:

- `TEXT`
- `BLOB`
- `INTEGER`
- `REAL`

*/

/**
 * `SqliteNativeType` is one of the four native types supported by sqlite.
 */
export type SqliteNativeType = "TEXT" | "BLOB" | "INTEGER" | "REAL";

/*

These four map *somewhat* cleanly onto Javascript types. Values can also be
NULL, but we're going to disregard that for now.

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

*/

type AutoIncrementableColumnSchema = {
  type: "INTEGER";

  /**
   * Whether this column's value should auto-increment.
   */
  autoIncrement: boolean;
};

export type ColumnSchema<
  T extends SqliteNativeType,
  ParsedType,
  Nullable extends boolean | undefined,
  DefaultValue = JsTypeForSqliteNativeType<T, true> | undefined,
> =
  | AutoIncrementableColumnSchema
  | {
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
      parse?: (value: JsTypeForSqliteNativeType<T, Nullable>) => ParsedType;

      /**
       * If provided, a function used to serialize native Javascript values
       * back to a form used by the database.
       */
      serialize?: (value: unknown) => JsTypeForSqliteNativeType<T, Nullable>;

      /**
       * Whether values in this column must be unique.
       * Defaults to `false`.
       */
      unique?: boolean;
    };

export type JsTypeForColumnSchema<
  ColumnSchemaType extends
    | SqliteNativeType
    | ColumnSchema<SqliteNativeType, any, any>,
> = ColumnSchemaType extends SqliteNativeType
  ? // We've been given a literal string, e.g. "TEXT"
    JsTypeForSqliteNativeType<ColumnSchemaType, false>
  : // We've been given a ColumnSchema, e.g. { "type": "TEXT" }
    ColumnSchemaType extends ColumnSchema<
        infer T,
        infer ParsedType,
        infer Nullable
      >
    ? ColumnSchemaType extends {
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
    : never;

export type TableSchema<ColumnNames extends string> = {
  columns: {
    [columnName in ColumnNames]:
      | ColumnSchema<SqliteNativeType, any, any, any>
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

type MapWhereNullableColumnsHaveColumnNameAsValue<
  T extends TableSchema<string>,
> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends SqliteNativeType
    ? never
    : T["columns"][columnName] extends ColumnSchema<any, any, true>
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
  [columnName in keyof Table["columns"]]: Table["columns"][columnName] extends SqliteNativeType
    ? JsTypeForSqliteNativeType<Table["columns"][columnName], false>
    : Table["columns"][columnName] extends ColumnSchema<
          infer Type,
          any,
          infer Nullable
        >
      ? JsTypeForSqliteNativeType<Type, Nullable>
      : never;
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

type HasAutoIncrementingColumn<T extends TableSchema<string>> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends {
    autoIncrement: true;
  }
    ? // Column auto-increments
      true
    : // Column does not auto-increment
      false;
}[keyof T["columns"]] extends true
  ? true
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

export type GreaterThanComparison<Table extends TableSchema<string>> = {
  gt: Table["columns"] | number;
};

export type GreaterThanOrEqualToComparison<Table extends TableSchema<string>> =
  {
    gte: Table["columns"] | number;
  };

export type LessThanComparison<Table extends TableSchema<string>> = {
  lt: Table["columns"] | number;
};

export type LessThanOrEqualToComparison<Table extends TableSchema<string>> = {
  eq: Table["columns"] | number;
};

// Track what kinds of values we accept for a column-based criteria
type ValueForCriteria<
  Table extends TableSchema<string>,
  ColumnName extends ColumnNames<Table>,
> =
  JsTypeForColumnSchema<Table["columns"][ColumnName]> extends string
    ? /* string */ string | string[] | undefined | null
    : JsTypeForColumnSchema<Table["columns"][ColumnName]> extends number
      ? /* number */
        | number
          | number[]
          | undefined
          | null
          | GreaterThanComparison<Table>
          | GreaterThanOrEqualToComparison<Table>
          | LessThanComparison<Table>
          | LessThanOrEqualToComparison<Table>
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

*/

const ERROR_CODES = [
  "INSERT_ERROR",
  "INVALID_SCHEMA",
  "NO_SUCH_TABLE",
  "UNIQUE_CONSTRAINT_VIOLATION",
  "UNKNOWN_ERROR",
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

export class NoSuchTableError extends SqliteDatastoreError {
  constructor(public readonly tableName: string) {
    super(`No such table: ${tableName}`, "NO_SUCH_TABLE");
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

export class SqliteDatastore<TSchema extends Schema> {
  readonly #filename: string;
  readonly #schema: TSchema;
  readonly #databasePromise: Promise<Database>;
  #migrated: boolean = false;

  constructor({ schema, filename, ...rest }: SqliteDatastoreOptions<TSchema>) {
    this.#filename = filename ?? ":memory:";
    this.#schema = schema;
    this.#databasePromise = new Promise((resolve, reject) => {
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

    return Promise.all([
      this.migrateIfNeeded(),
      this.prepare(sql.join(" "), params),
    ]).then(
      ([_, statement]) =>
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

    const columnNames = this.getColumnNamesForInsert<
      TSchema["tables"][TableName]
    >(tableName, tableSchema, records);

    const sql = [
      `INSERT INTO "${tableName}" `,
      "(",
      columnNames.map((c) => `"${String(c)}"`).join(","),
      ") VALUES (",
      columnNames.map(() => "?").join(","),
      ")",
    ].join("");

    const statement = await this.prepare(sql);

    return records
      .reduce<Promise<InsertResult<TSchema, TableName>>>(
        (promise, record) =>
          promise.then(async (result) => {
            const params = columnNames.map((columnName) => {
              const value = record[columnName as keyof typeof record];
              const columnSchema =
                tableSchema.columns[
                  columnName as keyof typeof tableSchema.columns
                ];

              if (
                typeof columnSchema === "object" &&
                "serialize" in columnSchema &&
                typeof columnSchema.serialize === "function"
              ) {
                return columnSchema.serialize(value);
              }

              return value;
            });

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
              reject(this.adaptSqliteError(err));
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

    const sql = [`SELECT * FROM "${tableName}"`];
    const params: unknown[] = [];

    const [whereClause, whereParams] = this.buildWhereClause(options.where);

    if (whereClause) {
      sql.push(whereClause);
      params.push(...whereParams);
    }

    type ResultRecord = RecordFor<TSchema["tables"][TableName]>;

    return Promise.all([
      this.migrateIfNeeded(),
      this.prepare(sql.join(" "), params),
    ])
      .then(
        ([_, statement]) =>
          new Promise<RecordFor<TSchema["tables"][TableName]>[]>(
            (resolve, reject) => {
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
            },
          ),
      )
      .catch((err) => Promise.reject(this.adaptSqliteError(err)));
  }

  protected adaptSqliteError(err: Error): Error {
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
          } else if (typeof value === "object") {
            if ("gt" in value) {
              params.push(value.gt);
              sql.push(`"${columnName}" > ?`);
            }

            if ("gte" in value) {
              sql.push(`"${columnName}" >= ?`);
              params.push(value.gte);
            }

            if ("lt" in value) {
              sql.push(`"${columnName}" < ?`);
              params.push(value.lt);
            }

            if ("lte" in value) {
              sql.push(`"${columnName}" <= ?`);
              params.push(value.lte);
            }
          } else if (value == null) {
            sql.push(`("${columnName}" IS NULL)`);
          } else {
            sql.push(`("${columnName}" = ?)`);
            params.push(value);
          }

          return [sql, params];
        },
        [[], []],
      );

      return [sql.join(" AND `"), params];
    }
  }

  protected createTable(
    tableName: string,
    tableSchema: TableSchema<string>,
  ): Promise<void> {
    const columns = Object.entries(tableSchema.columns).map(
      ([columnName, columnSchema]) => {
        columnSchema =
          typeof columnSchema === "string"
            ? { type: columnSchema }
            : columnSchema;

        const { type } = columnSchema;

        const isPrimaryKey = Array.isArray(tableSchema.primaryKey)
          ? tableSchema.primaryKey.includes(columnName)
          : tableSchema.primaryKey === columnName;

        const autoIncrement =
          "autoIncrement" in columnSchema && columnSchema.autoIncrement;
        const nullable = "nullable" in columnSchema && !!columnSchema.nullable;
        const unique = "unique" in columnSchema && !!columnSchema.unique;

        if (autoIncrement && !isPrimaryKey) {
          throw new InvalidSchemaError(
            `Column '${columnName}' in table '${tableName}' is marked as auto-incrementing but is not part of the table's primary key.`,
          );
        }

        return [
          `"${columnName}"`,
          type,
          isPrimaryKey && "PRIMARY KEY",
          autoIncrement && "AUTOINCREMENT",
          nullable ? "NULL" : "NOT NULL",
          (autoIncrement || unique) && "UNIQUE",
        ]
          .filter(Boolean)
          .join(" ");
      },
    );

    const sql = [
      `CREATE TABLE IF NOT EXISTS "${tableName}" (`,
      columns.join(", "),
      ")",
    ].join("");

    return this.executeSql(sql);
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
      const columnSchema =
        tableSchema.columns[columnName as keyof typeof tableSchema.columns];

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
    return this.#databasePromise.then(
      (db) =>
        new Promise((resolve, reject) => {
          db.prepare(sql, params, function (err) {
            if (err) {
              reject(err);
              return;
            }
            resolve(this);
          });
        }),
    );
  }

  private getColumnNamesForInsert<Table extends TableSchema<string>>(
    tableName: string,
    tableSchema: Table,
    records: InsertRecordFor<Table>[],
  ): ColumnNames<Table>[] {
    const validColumnNames = new Set<string>(Object.keys(tableSchema.columns));

    // Each incoming record may specify a different set of columns to insert.
    // We want to build a single INSERT statement to cover all cases.
    // So first, we need a set of all columns that will be inserted.
    // TODO: Provide an option ("unchecked"?) to allow faster inserts without
    // these pre-checks.
    const columnNameSet = new Set<keyof InsertRecordFor<Table>>();

    for (const record of records) {
      Object.keys(record).forEach((columnName) => {
        if (!validColumnNames.has(columnName)) {
          throw new InsertError(
            `Column '${columnName}' not found on table '${tableName}'`,
          );
        }

        columnNameSet.add(columnName as keyof InsertRecordFor<Table>);
      });

      if (columnNameSet.size === validColumnNames.size) {
        break;
      }
    }

    return Array.from(columnNameSet);
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
