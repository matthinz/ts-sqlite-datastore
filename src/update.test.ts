import assert from "node:assert";
import { describe, it } from "node:test";
import type { Schema } from "./sqlite-datastore.ts";
import { createDataStore } from "./test-utils.ts";

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

describe("#update", () => {
  describe("update all records in a table with an object", () => {
    it("updates all records", async () => {
      const [dataStore] = await createDataStore({
        schema: TEST_SCHEMA,
      });

      await dataStore.insert("people", { name: "foo" });
      await dataStore.insert("people", { name: "bar" });

      const result = await dataStore.update("people", {
        set: {
          name: "baz",
        },
      });
      assert.equal(result["count"], 2, "count");

      const records = await dataStore.select("people");
      assert.deepStrictEqual(records, [
        { id: 1, name: "baz", birthdate: null },
        { id: 2, name: "baz", birthdate: null },
      ]);
    });
  });

  it("updates some records using where clause", async () => {
    const [dataStore] = await createDataStore({
      schema: TEST_SCHEMA,
    });

    await dataStore.insert("people", { name: "foo" });
    await dataStore.insert("people", { name: "bar" });

    const result = await dataStore.update("people", {
      set: {
        name: "baz",
      },
      where: {
        name: "foo",
      },
    });

    assert.equal(result["count"], 1, "count");

    const records = await dataStore.select("people");

    assert.deepStrictEqual(records, [
      { id: 1, name: "baz", birthdate: null },
      { id: 2, name: "bar", birthdate: null },
    ]);
  });
});
