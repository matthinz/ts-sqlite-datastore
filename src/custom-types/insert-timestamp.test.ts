import assert from "node:assert";
import { describe, it } from "node:test";
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

        assert.deepStrictEqual(col, {
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

          assert.equal(records.length, 1, "1 record found");

          assert(records[0].created_at instanceof Date, "created_at is a Date");

          assert(
            !isNaN(Number(records[0].created_at)),
            "created_at is not NaN",
          );

          assert(
            Date.now() - records[0].created_at.getTime() < 1000,
            "created_at is within 1s of now",
          );
        }),
      );
    });

    describe("when Date provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await assert.rejects(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
            InsertError,
          );
        }),
      );
    });

    describe("when valid ISO date string provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await assert.rejects(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
            InsertError,
          );
        }),
      );
    });

    describe("when invalid value provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await assert.rejects(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
            InsertError,
          );
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

          assert.rejects(
            dataStore.update("people", {
              set: {
                created_at: new Date(),
              },
            }),
            UpdateError,
          );
        }),
      );
    });
  });
});
