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

describe("#count", () => {
  it(
    "returns the count of records",
    testWithSchema(TEST_SCHEMA, async (dataStore) => {
      await dataStore.insert("people", { name: "foo" });
      await dataStore.insert("people", { name: "bar" });
      await dataStore.insert("people", { name: "baz" });

      const count = await dataStore.count("people", {
        where: { name: ["foo", "bar"] },
      });

      expect(count).toEqual(2);
    }),
  );
});
