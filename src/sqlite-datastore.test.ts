import fs from "node:fs/promises";
import path from "node:path";
import { InvalidSchemaError, Schema } from "./sqlite-datastore";
import { all, createDataStore, testWithSchema } from "./test-utils";

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

    describe("with auto-increment column that is not primary key", () => {
      const SCHEMA = {
        tables: {
          people: {
            columns: {
              id: {
                type: "INTEGER",
                autoIncrement: true,
              },
              name: "TEXT",
            },
          },
        },
      } satisfies Schema;

      it("throws an Error", async () => {
        const [dataStore] = await createDataStore({
          schema: SCHEMA,
        });

        expect(dataStore.migrate()).rejects.toThrow(
          new InvalidSchemaError(
            "Column 'id' in table 'people' is marked as auto-incrementing but is not part of the table's primary key.",
          ),
        );
      });
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
