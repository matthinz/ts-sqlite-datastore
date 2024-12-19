import fs from "node:fs/promises";
import path from "node:path";
import { Database } from "sqlite3";
import {
  InsertError,
  InsertRecordFor,
  Schema,
  SqliteDatastore,
  SqliteDatastoreOptions,
} from "./sqlite-datastore";

const TEST_SCHEMA = {
  tables: {
    people: {
      columns: {
        id: {
          type: "INTEGER",
          autoIncrement: true,
        },
        name: "TEXT",
        birthdate: {
          type: "TEXT",
          nullable: true,
        },
      },
      primaryKey: "id",
    },
  },
} satisfies Schema;

type t = InsertRecordFor<(typeof TEST_SCHEMA)["tables"]["people"]>;

describe("SqliteDatastore", () => {
  describe("constructor", () => {
    it(
      "uses an in-memory database by default",
      testWithSchema(TEST_SCHEMA, async (dataStore) => {
        expect(dataStore.filename).toEqual(":memory:");
      }),
    );

    it("allows specifying filename", async () => {
      const dir = await fs.mkdtemp(".sqlite-datastore");
      const filename = path.join(dir, "__test.db");

      const [dataStore] = await createDataStore({
        filename,
        schema: TEST_SCHEMA,
      });

      try {
        expect(dataStore.filename).toEqual(filename);
      } finally {
        await fs.rm(dir, { recursive: true });
      }
    });
  });

  describe("#insert", () => {
    describe("with 1 record", () => {
      it(
        "inserts the record",
        testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
          const result = await dataStore.insert("people", {
            name: "Person A",
            birthdate: "2000-01-01",
          });

          expect(result).toEqual({
            count: 1,
            ids: [1],
          });

          const records = await all(db, "SELECT * FROM people");
          expect(records).toEqual([
            { id: 1, name: "Person A", birthdate: "2000-01-01" },
          ]);
        }),
      );
    });

    describe("with a record that includes columns not present in schema", () => {
      it(
        "throws an Error",
        testWithSchema(TEST_SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "Person A",
              birthdate: "2000-01-01",
              extra: "extra",
            } as InsertRecordFor<(typeof TEST_SCHEMA)["tables"]["people"]>),
          ).rejects.toThrow(
            new InsertError("Column 'extra' not found on table 'people'"),
          );
        }),
      );
    });

    describe("with an array of records", () => {
      it(
        "inserts records",
        testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
          const result = await dataStore.insert("people", [
            { name: "foo" },
            { name: "bar", birthdate: "2000-01-01" },
          ]);

          expect(result).toEqual({
            count: 2,
            ids: [1, 2],
          });

          const records = await all(db, "SELECT * FROM people");
          expect(records).toEqual([
            { id: 1, name: "foo", birthdate: null },
            { id: 2, name: "bar", birthdate: "2000-01-01" },
          ]);
        }),
      );
    });
  });

  describe("#migrate", () => {
    it(
      "creates tables",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.migrate();

        const actual = await all(
          db,
          "SELECT name FROM sqlite_master WHERE type='table';",
        );
        expect(actual).toEqual([
          { name: "people" },
          { name: "sqlite_sequence" },
        ]);
      }),
    );

    it(
      "is idempotent",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.migrate();
        await dataStore.migrate();
      }),
    );
  });
});

function createDataStore<TSchema extends Schema>(
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

function all(
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

function testWithSchema<TSchema extends Schema>(
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
