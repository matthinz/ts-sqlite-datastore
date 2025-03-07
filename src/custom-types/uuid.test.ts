import assert from "node:assert";
import { describe, it } from "node:test";
import type { Schema } from "../sqlite-datastore.ts";
import { InvalidUUIDError } from "../sqlite-datastore.ts";
import { all, testWithSchema } from "../test-utils.ts";

const SCHEMA = {
  tables: {
    people: {
      columns: {
        id: "uuid",
        name: "TEXT",
      },
      primaryKey: "id",
    },
  },
} satisfies Schema;

describe("uuid custom type", () => {
  describe("on insert", () => {
    describe("when not provided", () => {
      it(
        "generates a UUID",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", { name: "foo" });

          const actual = await all(db, "SELECT * FROM people");

          assert.equal(actual.length, 1);

          const { id } = actual[0] as any;

          assert.match(
            id,
            /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
          );
        }),
      );
    });

    describe("when value is a valid UUID", () => {
      it(
        "uses the specified value",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", {
            id: "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0",
            name: "foo",
          });

          const actual = await all(db, "SELECT * FROM people");

          assert.deepStrictEqual(actual, [
            {
              id: "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0",
              name: "foo",
            },
          ]);
        }),
      );
    });

    describe("when value is present but not a valid UUID", () => {
      it(
        "throws an InvalidUUIDError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await assert.rejects(
            dataStore.insert("people", {
              id: "not a uuid",
              name: "foo",
            }),
            InvalidUUIDError,
          );
        }),
      );
    });
  });

  describe("on update", () => {
    describe("when value is a valid UUID", () => {
      it(
        "uses the specified value",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });

          await dataStore.update("people", {
            set: {
              id: "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0",
            },
          });

          const records = await dataStore.select("people");

          assert.equal(records.length, 1, "1 record found");
          assert.equal(records[0].id, "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0");
        }),
      );
    });

    describe("when value is present but not a valid UUID", () => {
      it(
        "throws an InvalidUUIDError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", {
            name: "foo",
          });

          await assert.rejects(
            dataStore.update("people", { set: { id: "not a uuid" } }),
            InvalidUUIDError,
          );
        }),
      );
    });
  });
});
