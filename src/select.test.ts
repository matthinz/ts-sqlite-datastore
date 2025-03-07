import assert from "node:assert";
import { describe, it } from "node:test";
import type { Schema } from "./sqlite-datastore.ts";
import { NoSuchTableError } from "./sqlite-datastore.ts";
import { runSql, testWithSchema } from "./test-utils.ts";

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

describe("#select", () => {
  describe("select all", () => {
    it(
      "selects all records",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });

        const records = await dataStore.select("people");

        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: null },
          { id: 2, name: "bar", birthdate: null },
        ]);
      }),
    );
  });

  describe("select with single string criteria", () => {
    it(
      "selects records that match the condition",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });

        const records = await dataStore.select("people", {
          where: { name: "foo" },
        });

        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: null },
        ]);
      }),
    );
  });

  describe("select with single string array criteria", () => {
    it(
      "selects records that match the condition",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });
        await dataStore.insert("people", { name: "baz" });

        const records = await dataStore.select("people", {
          where: { name: ["foo", "bar"] },
        });

        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: null },
          { id: 2, name: "bar", birthdate: null },
        ]);
      }),
    );
  });

  describe("select by id", () => {
    describe("single value", () => {
      it(
        "selects the record",
        testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
          const result = await dataStore.insert("people", [
            { name: "foo" },
            { name: "bar" },
          ]);

          assert.equal(result["count"], 2, "count");
          assert.deepStrictEqual(result["ids"], [1, 2]);

          const records = await dataStore.select("people", {
            where: { id: 1 },
          });

          assert.deepStrictEqual(records, [
            { id: 1, name: "foo", birthdate: null },
          ]);
        }),
      );
    });

    describe("array of values", () => {});
  });

  describe("select with multiple criteria", () => {
    it(
      "selects records that match all conditions",

      testWithSchema(TEST_SCHEMA, async (dataStore) => {
        await dataStore.insert("people", {
          name: "foo",
          birthdate: "2000-01-01",
        });
        await dataStore.insert("people", {
          name: "bar",
          birthdate: "2000-01-01",
        });
        await dataStore.insert("people", {
          name: "baz",
          birthdate: "2000-01-02",
        });

        const records = await dataStore.select("people", {
          where: { name: "foo", birthdate: "2000-01-01" },
        });

        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: "2000-01-01" },
        ]);
      }),
    );
  });

  describe("select with criteria using non-nullable INTEGER column", () => {
    const SCHEMA = {
      tables: {
        people: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: "TEXT",
            age: "INTEGER",
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    it(
      "selects records that match the condition",
      testWithSchema(SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo", age: 20 });
        await dataStore.insert("people", { name: "bar", age: 30 });

        const records = await dataStore.select("people", {
          where: { age: 20 as number | bigint },
        });

        assert.deepStrictEqual(records, [{ id: 1, name: "foo", age: 20 }]);
      }),
    );
  });

  describe("select with LIKE operator", () => {
    it(
      "selects records that match the condition",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });
        await dataStore.insert("people", { name: "baz" });

        const records = await dataStore.select("people", {
          where: { name: { like: "ba%" } },
        });

        assert.deepStrictEqual(records, [
          { id: 2, name: "bar", birthdate: null },
          { id: 3, name: "baz", birthdate: null },
        ]);
      }),
    );
  });

  describe("select with eq operator", () => {
    it(
      "selects records that match the condition",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });
        await dataStore.insert("people", { name: "baz" });

        const records = await dataStore.select("people", {
          where: { name: { eq: "foo" } },
        });

        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: null },
        ]);
      }),
    );
  });

  describe("select with neq operator", () => {
    it(
      "selects records that match the condition",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.insert("people", { name: "foo" });
        await dataStore.insert("people", { name: "bar" });
        await dataStore.insert("people", { name: "baz" });

        const records = await dataStore.select("people", {
          where: { name: { neq: "foo" } },
        });

        assert.deepStrictEqual(records, [
          { id: 2, name: "bar", birthdate: null },
          { id: 3, name: "baz", birthdate: null },
        ]);
      }),
    );
  });

  describe("with custom parser on column", () => {
    const SCHEMA = {
      tables: {
        events: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: "TEXT",
            date: {
              type: "TEXT",
              nullable: false,
              parse: (value) => new Date(value as string),
            },
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    it(
      "parses the value",
      testWithSchema(SCHEMA, async (dataStore, db) => {
        await dataStore.insert("events", {
          name: "Birthday party",
          date: "2021-02-03 12:13:14",
        });

        const actual = await dataStore.select("events");

        assert.deepStrictEqual(actual, [
          {
            id: 1,
            name: "Birthday party",
            date: new Date("2021-02-03 12:13:14"),
          },
        ]);
      }),
    );
  });

  describe("with custom parser on nullable column", () => {
    const SCHEMA = {
      tables: {
        events: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: "TEXT",
            date: {
              type: "TEXT",
              nullable: true,
              parse: (value) =>
                value == null ? null : new Date(value as string),
            },
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    it(
      "parses the value",
      testWithSchema(SCHEMA, async (dataStore, db) => {
        await dataStore.insert("events", {
          name: "Date night",
        });

        const actual = await dataStore.select("events");

        assert.deepStrictEqual(actual, [
          {
            id: 1,
            name: "Date night",
            date: null,
          },
        ]);
      }),
    );
  });

  describe("when query fails", () => {
    it(
      "throws an appropriate error",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        await dataStore.migrate();
        await runSql(db, "DROP TABLE people");

        await assert.rejects(
          dataStore.select("people"),
          new NoSuchTableError("people"),
        );
      }),
    );
  });
});
