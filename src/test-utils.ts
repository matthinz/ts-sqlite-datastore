import sqlite3 from "sqlite3";
import type { Schema, SqliteDatastoreOptions } from "./sqlite-datastore.ts";
import { SqliteDatastore } from "./sqlite-datastore.ts";

export function all(
  db: sqlite3.Database,
  sql: string,
  ...params: unknown[]
): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, function (err, rows) {
      if (err) {
        reject(err);
        return;
      }
      resolve(rows);
    });
  });
}

export function createDataStore<TSchema extends Schema>(
  options: SqliteDatastoreOptions<TSchema>,
): Promise<[SqliteDatastore<TSchema>, sqlite3.Database]> {
  return new Promise((resolve, reject) => {
    const dataStore = new SqliteDatastore({
      ...options,
      onDatabaseReady(err: unknown, db?: sqlite3.Database) {
        if (err) {
          reject(err);
          return;
        }
        resolve([dataStore, db!]);
      },
    } as SqliteDatastoreOptions<TSchema>);
  });
}

export function runSql(
  db: sqlite3.Database,
  sql: string,
  ...params: unknown[]
): Promise<void> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
}

export function testWithSchema<TSchema extends Schema>(
  schema: TSchema,
  test: (
    dataStore: SqliteDatastore<TSchema>,
    db: sqlite3.Database,
  ) => Promise<void>,
): () => Promise<void> {
  return async () => {
    const [dataStore, db] = await createDataStore({
      schema,
    });
    try {
      await test(dataStore, db);
    } finally {
      await dataStore.close();
    }
  };
}
