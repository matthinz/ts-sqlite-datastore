import { Database } from "sqlite3";
import {
  Schema,
  SqliteDatastore,
  SqliteDatastoreOptions,
} from "../sqlite-datastore";

export function all(
  db: Database,
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
): Promise<[SqliteDatastore<TSchema>, Database]> {
  return new Promise((resolve, reject) => {
    const dataStore = new SqliteDatastore({
      ...options,
      onDatabaseReady(err, db) {
        if (err) {
          reject(err);
          return;
        }
        resolve([dataStore, db!]);
      },
    });
  });
}

export function testWithSchema<TSchema extends Schema>(
  schema: TSchema,
  test: (dataStore: SqliteDatastore<TSchema>, db: Database) => Promise<void>,
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
