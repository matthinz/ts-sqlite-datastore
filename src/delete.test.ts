import assert from "node:assert";
import { describe, it } from "node:test";
import type { Schema } from "./sqlite-datastore.ts";
import { testWithSchema } from "./test-utils.ts";

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

describe("#delete", () => {
  describe("with { all: true }", () => {
    it(
      "deletes all records",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });

        const result = await dataStore.delete("people", { all: true });
        assert.equal(result["count"], 2, "count");

        const records = await dataStore.select("people");
        assert.deepStrictEqual(records, []);
      }),
    );
  });

  describe("with { where: ... }", () => {
    it(
      "deletes all records",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });

        const result = await dataStore.delete("people", {
          where: {
            name: "foo",
          },
        });

        assert.equal(result["count"], 1, "count");

        const records = await dataStore.select("people");
        assert.deepStrictEqual(records, [
          { id: 2, name: "bar", birthdate: null },
        ]);
      }),
    );
  });
});
