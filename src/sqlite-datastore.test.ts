import { Database, RunResult } from "sqlite3";
import fs from "node:fs/promises";
import path from "node:path";
import {
  SqliteDatastore,
  SqliteDatastoreOptions,
  Tables,
} from "./sqlite-datastore";

const TEST_TABLES: Tables = {
  test: {
    columns: {
      id: "TEXT",
      name: "TEXT",
      version: "INTEGER",
    },
  },
};

describe("SqliteDatastore", () => {
  let dataStore: SqliteDatastore<typeof TEST_TABLES>;
  let db: Database;

  beforeEach(async () => {
    [dataStore, db] = await createDataStore({
      tables: TEST_TABLES,
    });
  });

  afterEach(async () => {
    await dataStore.close();
  });

  describe("constructor", () => {
    it("uses an in-memory database by default", async () => {
      const rows = await all(db, "PRAGMA database_list;");
      expect(rows).toHaveLength(1);
      expect((rows[0] as any).file).toEqual("");
    });

    it("allows specifying filename", async () => {
      const dir = await fs.mkdtemp(".sqlite-datastore");
      const filename = path.join(dir, "__test.db");

      let localDataStore: SqliteDatastore<typeof TEST_TABLES>;
      let localDb: Database;

      try {
        [localDataStore, localDb] = await createDataStore({
          tables: TEST_TABLES,
          filename,
        });

        const rows = await all(db, "PRAGMA database_list;");
        expect(rows).toHaveLength(1);

        const { file } = rows[0] as any;
        expect(file).toEqual(path.resolve(filename));
      } finally {
        await localDataStore!.close();
        await fs.rmdir(dir, { recursive: true });
      }
    });
  });

  describe("#insert", () => {
    describe("with 1 record", () => {
      it("inserts the record", async () => {
        const result = await dataStore.insert("test", {
          id: "1234",
        });

        expect(result).toEqual({
          count: 1,
          lastID: 1,
        });

        const records = await all(db, "SELECT * FROM test");
        expect(records).toEqual([{ id: "1234", name: null, version: null }]);
      });
    });

    describe("with an array of records", () => {
      it("inserts records", async () => {
        const result = await dataStore.insert("test", [
          { id: "1234" },
          { id: "5678" },
        ]);

        expect(result).toEqual({
          count: 2,
          lastID: 2,
        });

        const records = await all(db, "SELECT * FROM test");
        expect(records).toEqual([
          { id: "1234", name: null, version: null },
          { id: "5678", name: null, version: null },
        ]);
      });

      describe("with different columns specified", () => {
        it("does not fail", async () => {
          await dataStore.insert("test", [
            { id: "1234", name: "foo" },
            { id: "5678", version: 2 },
          ]);
          const records = await all(db, "SELECT * FROM test");
          expect(records).toEqual([
            { id: "1234", name: "foo", version: null },
            { id: "5678", name: null, version: 2 },
          ]);
        });
      });
    });
  });

  describe("#migrate", () => {
    it("creates tables", async () => {
      await dataStore.migrate();

      const actual = await all(
        db,
        "SELECT name FROM sqlite_master WHERE type='table';",
      );
      expect(actual).toEqual([{ name: "test" }]);
    });
    it("is idempotent", async () => {
      await dataStore.migrate();
      await dataStore.migrate();
    });
  });
});

function createDataStore<TTables extends Tables>(
  options: SqliteDatastoreOptions<TTables>,
): Promise<[SqliteDatastore<TTables>, Database]> {
  return new Promise((resolve, reject) => {
    const dataStore = new SqliteDatastore({
      ...options,
      internals(db) {
        setImmediate(resolve, [dataStore, db]);
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

function run(
  db: Database,
  sql: string,
  ...params: unknown[]
): Promise<RunResult> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        reject(err);
        return;
      }

      resolve(this);
    });
  });
}
