import { NoSuchTableError, Schema } from "./sqlite-datastore";
import { runSql, testWithSchema } from "./test-utils";

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

        expect(records).toEqual([
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

        expect(records).toEqual([{ id: 1, name: "foo", birthdate: null }]);
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

        expect(records).toEqual([
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

          expect(result).toHaveProperty("count", 2);
          expect(result).toHaveProperty("ids", [1, 2]);

          const records = await dataStore.select("people", {
            where: { id: 1 },
          });

          expect(records).toEqual([{ id: 1, name: "foo", birthdate: null }]);
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

        expect(records).toEqual([
          { id: 1, name: "foo", birthdate: "2000-01-01" },
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

        expect(actual).toEqual([
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

        expect(actual).toEqual([
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

        await expect(dataStore.select("people")).rejects.toThrow(
          new NoSuchTableError("people"),
        );
      }),
    );
  });
});
