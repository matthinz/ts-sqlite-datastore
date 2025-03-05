import fs from "node:fs/promises";
import path from "node:path";
import type { Schema } from "./sqlite-datastore.ts";
import { InvalidSchemaError } from "./sqlite-datastore.ts";
import { all, createDataStore, testWithSchema } from "./test-utils.ts";

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
              foo: {
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
            "Column 'foo' in table 'people' is marked as auto-incrementing but is not part of the table's primary key.",
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

    describe("when an invalid type specified", () => {
      const SCHEMA = {
        tables: {
          people: {
            columns: {
              id: "INTEGER",
              name: "INVALID",
            },
          },
        },
      } satisfies Schema;

      it(
        "throws an Error",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(dataStore.migrate()).rejects.toThrow(
            new InvalidSchemaError("Invalid column type: INVALID"),
          );
        }),
      );
    });

    describe("when no primary key specified", () => {
      const SCHEMA = {
        tables: {
          users: {
            columns: {
              id: "INTEGER",
              name: "TEXT",
            },
          },
        },
      } satisfies Schema;

      it(
        "uses id by default",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.migrate();

          // assert that id is the primary key of the users table
          const actual = await all(db, "PRAGMA table_info(users);");
          expect(actual).toEqual([
            {
              cid: 0,
              name: "id",
              type: "INTEGER",
              notnull: 1,
              dflt_value: null,
              pk: 1,
            },
            {
              cid: 1,
              name: "name",
              type: "TEXT",
              notnull: 1,
              dflt_value: null,
              pk: 0,
            },
          ]);
        }),
      );

      describe("when there is no column called id", () => {
        const SCHEMA = {
          tables: {
            users: {
              columns: {
                name: "TEXT",
              },
            },
          },
        } satisfies Schema;

        it(
          "does not add a primary key",
          testWithSchema(SCHEMA, async (dataStore, db) => {
            await dataStore.migrate();

            const actual = await all(db, "PRAGMA table_info(users);");
            expect(actual).toEqual([
              {
                cid: 0,
                name: "name",
                type: "TEXT",
                notnull: 1,
                dflt_value: null,
                pk: 0,
              },
            ]);
          }),
        );
      });
    });
  });
});
