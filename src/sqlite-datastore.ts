import { Database, Statement } from "sqlite3";

/*

# ts-sqlite-datastore

*/

/*

## Column types

sqlite represents data using four types:

- TEXT
- BLOB
- INTEGER
- REAL

*/

/**
 * One of the four native types supported by sqlite.
 */
export type SqliteNativeType = "TEXT" | "BLOB" | "INTEGER" | "REAL";

/*

These four map *somewhat* cleanly onto Javascript types. Values can also be
NULL, but we're going to disregard that for now.

The JsTypeForSqliteNativeType helper allows us to convert between sqlite types
and Javascript types. We also optionally allow specifying nullability here.

*/

export type JsTypeForSqliteNativeType<
  T extends SqliteNativeType,
  Nullable extends boolean = false,
> = Nullable extends true
  ? T extends "TEXT"
    ? string | void
    : T extends "BLOB"
      ? Buffer | string | void
      : T extends "INTEGER"
        ? number | bigint | void
        : T extends "REAL"
          ? number | void
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

Using one of the four native types, we can describe a **Column** in a database.

*/

export type ColumnSchema<
  T extends SqliteNativeType,
  Nullable extends boolean = false,
  DefaultValue = JsTypeForSqliteNativeType<T, true>,
> = {
  type: T;

  /**
   * Value to insert as a default.
   */
  defaultValue?: DefaultValue;

  /**
   * Whether this column can contain NULL values.
   */
  nullable: Nullable;

  /**
   * If provided, a function used to parse data from the database into a
   * native Javascript representation.
   */
  parse?: (input: unknown) => JsTypeForSqliteNativeType<T, Nullable>;

  /**
   * If provided, a function used to serialize native Javascript values
   * back to a form used by the database.
   */
  serialize?: (value: JsTypeForSqliteNativeType<T, Nullable>) => unknown;

  /**
   * Whether values in this column must be unique.
   * Defaults to `false`.
   */
  unique?: boolean;
};

/*

(Later we'll want to be able to extract a Javascript type from a Column schema.)

*/

export type JsTypeForColumnSchema<
  T extends ColumnSchema<SqliteNativeType, boolean>,
> = JsTypeForSqliteNativeType<T["type"], T["nullable"]>;

/*

A Table is composed of one or more Columns.

*/

export type TableSchema = {
  columns: {
    [columnName: string]:
      | ColumnSchema<SqliteNativeType, boolean>
      | SqliteNativeType;
  };
};

/*

We will need some utility types for this next bit.

*/

/**
 * Make some keys in T optional.
 */
type MakeOptional<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>;

/**
 * Extract only the nullable column names from a TableSchema.
 */
type NullableColumnNames<T extends TableSchema> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends SqliteNativeType
    ? never
    : T["columns"][columnName] extends ColumnSchema<any, true>
      ? columnName
      : never;
}[keyof T["columns"]];

/**
 * Extract only the names of columns that have default values set
 */
type NamesOfColumnsWithDefaultValues<T extends TableSchema> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends {
    defaultValue: any;
  }
    ? columnName
    : never;
}[keyof T["columns"]];

/*

We'll want to be able to derive a Javascript type for the records a Table
contains.

*/

export type RecordFor<T extends TableSchema> = {
  [columnName in keyof T["columns"]]: T["columns"][columnName] extends SqliteNativeType
    ? JsTypeForSqliteNativeType<T["columns"][columnName]>
    : T["columns"][columnName] extends ColumnSchema<infer Type, infer Nullable>
      ? JsTypeForSqliteNativeType<Type, Nullable>
      : never;
};

/*

When a column has a default value defined, it doesn't need to be specified
on insert. InsertRecordFor<T> returns a Record type for the given table, with
all columns that have default values marked as Optional.
*/

export type InsertRecordFor<T extends TableSchema> = MakeOptional<
  RecordFor<T>,
  NullableColumnNames<T> | NamesOfColumnsWithDefaultValues<T>
>;

/*

A Schema, then, is a set of Tables.

*/

export type Schema = {
  tables: {
    [tableName: string]: TableSchema;
  };
};

/*

We want to be able to pull out the Table names in a schema

*/

export type TableNames<TSchema extends Schema> = keyof TSchema["tables"];

/*

## Creating a SqliteDatastore

*/

export type SqliteDatastoreOptions<TSchema extends Schema> = {
  schema: TSchema;

  /**
   * If provided, the name of the sqlite database file to open.
   * If not provided, an in-memory database will be used.
   */
  filename?: string;

  /**
   * A hook to allow the caller to obtain the Database instance we are working with.
   * @param err
   * @param db
   */
  onDatabaseReady?: {
    (err: Error, db: void): void;
    (err: null, db: Database): void;
  };
};

/*

## Inserting records

We want to be able to cover a few different insertion scenarios:

1. Insert a single record
2. Insert multiple records

*/

/**
 * The value returned by an insert operation.
 */
export type InsertResult = {
  /**
   * The number of records inserted.
   */
  readonly count: number;

  /**
   * For Tables with an auto-incrementing ID column, the set of IDs that were
   * generated.
   */
  readonly ids: Set<number>;
};

export type InsertOptions<Table extends TableSchema> = {
  records: InsertRecordFor<Table>[];
};

/*

## Implementation

*/

export class SqliteDatastore<TSchema extends Schema> {
  readonly #filename: string;
  readonly #schema: TSchema;
  readonly #databasePromise: Promise<Database>;
  #migrated: boolean = false;

  constructor({
    schema,
    filename,
    onDatabaseReady,
  }: SqliteDatastoreOptions<TSchema>) {
    this.#filename = filename ?? ":memory:";
    this.#schema = schema;
    this.#databasePromise = new Promise((resolve, reject) => {
      const db = new Database(this.#filename, (err) => {
        onDatabaseReady = onDatabaseReady ?? (() => {});

        if (err) {
          setImmediate(
            onDatabaseReady as (err: Error | null, db?: Database) => void,
            err,
          );
          reject(err);
          return;
        }

        setImmediate(
          onDatabaseReady as (err: Error | null, db?: Database) => void,
          null,
          db,
        );
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

  /**
   * Inserts multiple records into the given table.
   * @param tableName Table to insert into.
   * @param records The set of records to insert.
   * @returns {Promise<InsertResult>} A structure describing how the insert went.
   */
  async insert<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    record: InsertRecordFor<TSchema["tables"][TableName]>,
  ): Promise<InsertResult>;
  async insert<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    records: InsertRecordFor<TSchema["tables"][TableName]>[],
  ): Promise<InsertResult>;
  async insert<TableName extends TableNames<TSchema>>(
    tableName: TableName,
    recordOrRecords:
      | InsertRecordFor<TSchema["tables"][TableName]>
      | InsertRecordFor<TSchema["tables"][TableName]>[],
  ) {
    await this.migrateIfNeeded();

    const records = Array.isArray(recordOrRecords)
      ? recordOrRecords
      : [recordOrRecords];

    // Allow different records to specify different
    // column names
    const columnNameSet = new Set<
      keyof InsertRecordFor<TSchema["tables"][TableName]>
    >();
    records.forEach((record) => {
      Object.keys(record).forEach((columnName) =>
        columnNameSet.add(
          columnName as keyof InsertRecordFor<TSchema["tables"][TableName]>,
        ),
      );
    });

    const columnNames = Array.from(columnNameSet);

    const sql = [
      `INSERT INTO "${String(tableName)}" `,
      "(",
      columnNames.map((c) => `"${String(c)}"`).join(","),
      ") VALUES (",
      columnNames.map(() => "?").join(","),
      ")",
    ].join("");

    const statement = await this.prepare(sql);

    return records
      .reduce<Promise<InsertResult>>(
        (promise, record) =>
          promise.then((result) => {
            const params = columnNames.map((c) => record[c]);

            return new Promise((resolve, reject) => {
              statement.run(params, function (err) {
                if (err) {
                  reject(err);
                  return;
                }

                (result as any).count += 1;

                if (this.lastID > 0) {
                  result.ids.add(this.lastID);
                }

                resolve(result);
              });
            });
          }),
        Promise.resolve({
          count: 0,
          ids: new Set(),
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
      );
  }

  migrate(): Promise<void> {
    return this.migrateIfNeeded().then(() => {});
  }

  protected createTable(
    tableName: string,
    tableSchema: TableSchema,
  ): Promise<void> {
    const columns = Object.entries(tableSchema.columns).map(
      ([columnName, columnSchema]) => {
        columnSchema =
          typeof columnSchema === "string"
            ? { type: columnSchema, nullable: false }
            : columnSchema;

        const { type } = columnSchema;

        return [
          `"${columnName}"`,
          type,
          columnSchema.nullable && "NULL",
          columnSchema.unique && "UNIQUE",
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
    return this.prepare(sql).then(
      (statement) =>
        new Promise((resolve, reject) => {
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

  protected prepare(sql: string): Promise<Statement> {
    return this.#databasePromise.then(
      (db) =>
        new Promise((resolve, reject) => {
          db.prepare(sql, function (err) {
            if (err) {
              reject(err);
              return;
            }
            resolve(this);
          });
        }),
    );
  }
}

function openDatabase(filename: string): Promise<Database> {
  return new Promise((resolve, reject) => {
    const db = new Database(filename, (err) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(db);
    });
  });
}
