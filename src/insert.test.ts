import assert from "node:assert";
import { describe, it } from "node:test";
import type { InsertRecordFor, Schema } from "./sqlite-datastore.ts";
import {
  InsertError,
  UniqueConstraintViolationError,
} from "./sqlite-datastore.ts";
import { all, testWithSchema } from "./test-utils.ts";

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

describe("#insert", () => {
  describe("with 1 record", () => {
    it(
      "inserts the record",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        const result = await dataStore.insert("people", {
          name: "Person A",
          birthdate: "2000-01-01",
        });

        assert.deepStrictEqual(result, {
          count: 1,
          ids: [1],
        });

        const records = await all(db, "SELECT * FROM people");
        assert.deepStrictEqual(records, [
          { id: 1, name: "Person A", birthdate: "2000-01-01" },
        ]);
      }),
    );
  });

  describe("with a record that includes columns not present in schema", () => {
    it(
      "throws an Error",
      testWithSchema(TEST_SCHEMA, async (dataStore) => {
        await assert.rejects(
          dataStore.insert("people", {
            name: "Person A",
            birthdate: "2000-01-01",
            extra: "extra",
          } as InsertRecordFor<(typeof TEST_SCHEMA)["tables"]["people"]>),
          new InsertError("Column 'extra' not found on table 'people'"),
        );
      }),
    );
  });

  describe("with an array of records", () => {
    it(
      "inserts records",
      testWithSchema(TEST_SCHEMA, async (dataStore, db) => {
        const result = await dataStore.insert("people", [
          { name: "foo" },
          { name: "bar", birthdate: "2000-01-01" },
        ]);

        assert.deepStrictEqual(result, {
          count: 2,
          ids: [1, 2],
        });

        const records = await all(db, "SELECT * FROM people");
        assert.deepStrictEqual(records, [
          { id: 1, name: "foo", birthdate: null },
          { id: 2, name: "bar", birthdate: "2000-01-01" },
        ]);
      }),
    );
  });

  describe("with custom serializer (same type) on column", () => {
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
              serialize: (value: unknown) =>
                new Date(String(value)).toISOString(),
            },
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    it(
      "serializes the value",
      testWithSchema(SCHEMA, async (dataStore, db) => {
        await dataStore.insert("events", {
          name: "Birthday party",
          date: "2021-02-03 12:13:14Z",
        });

        const actual = await all(db, "SELECT * FROM events");

        assert.deepStrictEqual(actual, [
          {
            id: 1,
            name: "Birthday party",
            date: "2021-02-03T12:13:14.000Z",
          },
        ]);
      }),
    );
  });

  describe("violating uniqueness constraint", () => {
    const SCHEMA = {
      tables: {
        people: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: { type: "TEXT", unique: true },
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    it(
      "throws an Error",
      testWithSchema(SCHEMA, async (dataStore) => {
        await dataStore.insert("people", { name: "foo" });

        await assert.rejects(
          dataStore.insert("people", { name: "foo" }),
          new UniqueConstraintViolationError("people", "name"),
        );
      }),
    );
  });

  describe("with a column that has a default value", () => {
    const SCHEMA = {
      tables: {
        people: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: { type: "TEXT", defaultValue: "John Doe" },
          },
          primaryKey: "id",
        },
      },
    };

    describe("when value is not specified", () => {
      it(
        "uses the default",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", {});

          const records = await dataStore.select("people");

          assert.equal(records.length, 1, "1 record found");
          assert.equal(records[0].name, "John Doe");
        }),
      );
    });

    describe("when value is specified", () => {
      it(
        "uses the specified value",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", { name: "Jane Doe" });

          const records = await dataStore.select("people");

          assert.equal(records.length, 1, "1 record found");
          assert.equal(records[0].name, "Jane Doe");
        }),
      );
    });
  });
});
