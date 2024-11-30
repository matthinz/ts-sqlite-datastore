import { table } from "node:console";
import { Database, RunResult, Statement } from "sqlite3";

/**
 * sqlite supports four native column types.
 */
export type SqliteNativeType = "TEXT" | "INTEGER" | "REAL" | "BLOB";

/**
 * These are additional "types" that SqliteDatastore provides.
 * They are implemented in Javascript and use sqlite native types
 * under the hood.
 *
 * These custom types are:
 *
 *   - "date-time" - A Date. Written and read in UTC.
 *   - "json" - A JSON value.
 *   - "id": An autoincrementing integer ID
 *   - "uuid" - A universally unique identifier.
 */
export type ExtraType = "date-time" | "json" | "id" | "uuid";

/**
 * A default value for a column.
 *
 * - "timestamp" means use the current date/time.
 * - { literal: x } means use the literal value "X".
 */
export type DefaultValue = "timestamp" | { literal: unknown };

type RecordFor<TTable extends TableSpec> = {
  [key in keyof TTable["columns"]]: unknown;
};

export type ColumnSpec = {
  type: SqliteNativeType | ExtraType;

  /**
   * The default value for this column.
   */
  defaultValue?: DefaultValue;

  /**
   * Whether this column should allow NULL values.
   */
  nullable?: boolean;

  /**
   * Whether values in this column should be unique.
   */
  unique?: boolean;
};

/**
 * Describes an index in the database.
 */
export type IndexSpec = {
  columns: string[];
  unique?: boolean;
};

/**
 * Describes the columns and indices for a table.
 */
export type TableSpec = {
  columns: {
    [columName: string]: ColumnSpec | SqliteNativeType;
  };
  indexes?: {
    [indexName: string]: IndexSpec;
  };
};

/**
 * My livelihood. A map of table names to their columns.
 */
export type Tables = {
  [tableName: string]: TableSpec;
};

export type TableNames<TTables extends Tables> = keyof TTables;

export type ColumnNames<TTable extends TableSpec> = keyof TTable["columns"];

type InsertResult = {
  count: number;
  lastID: number;
};

export type SqliteDatastoreOptions<TTables extends Tables> = {
  tables: TTables;

  filename?: string;

  /**
   * If provided, this callback will be invoked with the Database object
   * when it is available. This functionality is intended for test support.
   * @param db
   * @returns {void}
   */
  internals?: (db: Database) => void;
};

export class SqliteDatastore<TTables extends Tables> {
  #tables: TTables;
  #databasePromise: Promise<Database>;
  #migrated: boolean = false;

  constructor({
    tables,
    filename,
    internals,
  }: SqliteDatastoreOptions<TTables>) {
    this.#tables = tables;
    this.#databasePromise = openDatabase(filename ?? ":memory:");

    if (internals) {
      this.#databasePromise.then((db) => {
        setImmediate(internals, db);
      });
    }
  }

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

  async delete<TableName extends TableNames<TTables>>() {
    throw new Error();
  }

  async insert<TableName extends TableNames<TTables>>(
    tableName: TableName,
    records: RecordFor<TTables[TableName]>[],
  ): Promise<InsertResult>;
  async insert<TableName extends TableNames<TTables>>(
    tableName: TableName,
    record: RecordFor<TTables[TableName]>,
  ): Promise<InsertResult>;
  async insert<TableName extends TableNames<TTables>>(
    tableName: TableName,
    recordOrRecords:
      | RecordFor<TTables[TableName]>
      | RecordFor<TTables[TableName]>[],
  ) {
    await this.migrateIfNeeded();

    const records = Array.isArray(recordOrRecords)
      ? recordOrRecords
      : [recordOrRecords];

    // Allow different records to specify different
    // column names
    const columnNameSet = new Set<string>();
    records.forEach((record) => {
      Object.keys(record).forEach((columnName) =>
        columnNameSet.add(columnName),
      );
    });

    const columnNames = Array.from(columnNameSet);

    const sql = [
      `INSERT INTO "${String(tableName)}" `,
      "(",
      columnNames.map((c) => `"${c}"`).join(","),
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

                result.count += 1;
                result.lastID = this.lastID;

                resolve(result);
              });
            });
          }),
        Promise.resolve({
          count: 0,
          lastID: 0,
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

  async select<TableName extends TableNames<TTables>>(tableName: TableName) {}

  async update<TableName extends TableNames<TTables>>() {}

  protected createTable(tableName: string, spec: TableSpec): Promise<void> {
    const columns = Object.entries(spec.columns).map(
      ([columnName, columnSpec]) => {
        columnSpec =
          typeof columnSpec === "string" ? { type: columnSpec } : columnSpec;

        const { type } = columnSpec;

        return [
          `"${columnName}"`,
          type,
          columnSpec.nullable && "NULL",
          columnSpec.unique && "UNIQUE",
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

  protected executeStatement(
    statement: Statement,
    ...params: unknown[]
  ): Promise<RunResult> {
    throw new Error();
  }

  protected migrateIfNeeded(): Promise<Database> {
    return this.#databasePromise.then(async (db) => {
      if (this.#migrated) {
        return db;
      }

      this.#migrated = true;

      // Create tables
      await Object.entries(this.#tables).reduce<Promise<void>>(
        (promise, [tableName, tableSpec]) =>
          promise.then(() => this.createTable(tableName, tableSpec)),
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
