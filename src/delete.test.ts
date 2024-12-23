import { Schema } from "./sqlite-datastore";
import { testWithSchema } from "./test-utils";

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
  describe("delete all records in a table", () => {
    it(
      "deletes all records",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });

        const result = await dataStore.delete("people", { all: true });
        expect(result).toHaveProperty("count", 2);

        const records = await dataStore.select("people");
        expect(records).toEqual([]);
      }),
    );
  });

  describe("delete only some records in a table", () => {
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

        expect(result).toHaveProperty("count", 1);

        const records = await dataStore.select("people");
        expect(records).toEqual([{ id: 2, name: "bar", birthdate: null }]);
      }),
    );
  });
});
