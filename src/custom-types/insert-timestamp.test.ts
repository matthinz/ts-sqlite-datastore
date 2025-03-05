import type { Schema } from "../sqlite-datastore.ts";
import { InsertError, UpdateError } from "../sqlite-datastore.ts";
import { all, testWithSchema } from "../test-utils.ts";

const SCHEMA = {
  tables: {
    people: {
      columns: {
        id: {
          type: "INTEGER",
          autoIncrement: true,
        },
        name: "TEXT",
        created_at: "insert_timestamp",
      },
      primaryKey: "id",
    },
  },
} satisfies Schema;

describe("insert_timestamp custom type", () => {
  describe("schema", () => {
    it(
      "creates column with TEXT type",
      testWithSchema(SCHEMA, async (dataStore, db) => {
        await dataStore.migrate();

        const columns = await all(db, `PRAGMA table_info(people)`);
        const col = columns.find((c) => (c as any).name === "created_at");

        expect(col).toEqual({
          cid: 2,
          name: "created_at",
          type: "TEXT",
          notnull: 1,
          dflt_value: null,
          pk: 0,
        });
      }),
    );
  });

  describe("on insert", () => {
    describe("when no value provided for created_at", () => {
      it(
        "uses the current date/time",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", { name: "foo" });

          const records = await dataStore.select("people");

          expect(records).toHaveLength(1);

          expect(records[0]).toHaveProperty("created_at");
          expect(records[0].created_at).toBeInstanceOf(Date);
          expect(records[0].created_at).not.toBeNaN();

          expect(Date.now() - records[0].created_at.getTime()).toBeLessThan(
            1000,
          );
        }),
      );
    });

    describe("when Date provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });

    describe("when valid ISO date string provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });

    describe("when invalid value provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });
  });

  describe("on update", () => {
    describe("when value provided for created_at", () => {
      it(
        "throws an UpdateError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });

          expect(
            dataStore.update("people", {
              set: {
                created_at: new Date(),
              },
            }),
          ).rejects.toThrow(UpdateError);
        }),
      );
    });
  });
});
