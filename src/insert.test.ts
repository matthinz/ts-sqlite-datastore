import {
  InsertError,
  InsertRecordFor,
  InvalidUUIDError,
  Schema,
  SerializationError,
  UniqueConstraintViolationError,
} from "./sqlite-datastore";
import { all, testWithSchema } from "./test-utils";

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

        expect(result).toEqual({
          count: 1,
          ids: [1],
        });

        const records = await all(db, "SELECT * FROM people");
        expect(records).toEqual([
          { id: 1, name: "Person A", birthdate: "2000-01-01" },
        ]);
      }),
    );
  });

  describe("with a record that includes columns not present in schema", () => {
    it(
      "throws an Error",
      testWithSchema(TEST_SCHEMA, async (dataStore) => {
        await expect(
          dataStore.insert("people", {
            name: "Person A",
            birthdate: "2000-01-01",
            extra: "extra",
          } as InsertRecordFor<(typeof TEST_SCHEMA)["tables"]["people"]>),
        ).rejects.toThrow(
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

        expect(result).toEqual({
          count: 2,
          ids: [1, 2],
        });

        const records = await all(db, "SELECT * FROM people");
        expect(records).toEqual([
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

        expect(actual).toEqual([
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

        await expect(
          dataStore.insert("people", { name: "foo" }),
        ).rejects.toThrow(new UniqueConstraintViolationError("people", "name"));
      }),
    );
  });

  describe("with UUID type", () => {
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

    describe("when not provided", () => {
      it(
        "generates a UUID",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", { name: "foo" });

          const actual = await all(db, "SELECT * FROM people");

          expect(actual).toHaveLength(1);

          const { id } = actual[0] as any;

          expect(id).toMatch(
            /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
          );
        }),
      );
    });

    describe("when provided and looks valid", () => {
      it(
        "generates a UUID",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", {
            id: "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0",
            name: "foo",
          });

          const actual = await all(db, "SELECT * FROM people");

          expect(actual).toEqual([
            {
              id: "EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0",
              name: "foo",
            },
          ]);
        }),
      );
    });

    describe("when provided but not valid", () => {
      it(
        "generates a UUID",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          // use jest to assert that an InvalidUUIDError is thrown
          await expect(
            dataStore.insert("people", {
              id: "not a uuid",
              name: "foo",
            }),
          ).rejects.toThrow(InvalidUUIDError);
        }),
      );
    });
  });

  describe("with insert_timestamp type", () => {
    const SCHEMA = {
      tables: {
        people: {
          columns: {
            id: "uuid",
            name: "TEXT",
            created_at: "insert_timestamp",
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    describe("when no value provided for created_at", () => {
      it(
        "uses the current date/time",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await dataStore.insert("people", { name: "foo" });

          const actual = await all(db, "SELECT * FROM people");

          expect(actual).toHaveLength(1);

          const { created_at: createdAt } = actual[0] as any;

          expect(createdAt).not.toBeFalsy();

          const date = new Date(createdAt);
          expect(date).not.toBeNaN();
        }),
      );
    });

    describe("when Date provided for created_at", () => {
      it(
        "uses the date",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          const date = new Date();

          await dataStore.insert("people", { name: "foo", created_at: date });

          const actual = await all(db, "SELECT * FROM people");

          expect(actual).toHaveLength(1);

          const actualCreatedAt = new Date((actual[0] as any).created_at);

          expect(actualCreatedAt).toEqual(date);
        }),
      );
    });

    describe("when valid ISO date string provided for created_at", () => {
      it(
        "uses the date",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          const date = new Date();

          await dataStore.insert("people", {
            name: "foo",
            created_at: date.toISOString(),
          });

          const actual = await all(db, "SELECT * FROM people");

          expect(actual).toHaveLength(1);

          const actualCreatedAt = new Date((actual[0] as any).created_at);

          expect(actualCreatedAt).toEqual(date);
        }),
      );
    });

    describe("when invalid value provided for created_at", () => {
      it(
        "throws a SerializationError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await expect(async () => {
            await dataStore.insert("people", {
              name: "foo",
              created_at: "i like pie",
            });
          }).rejects.toThrow(SerializationError);
        }),
      );
    });
  });

  describe("with update_timestamp type", () => {
    const SCHEMA = {
      tables: {
        people: {
          columns: {
            id: {
              type: "INTEGER",
              autoIncrement: true,
            },
            name: "TEXT",
            updated_at: "update_timestamp",
          },
          primaryKey: "id",
        },
      },
    } satisfies Schema;

    describe("when specified in insert", () => {
      it(
        "ignores the specified value and sets to the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
            updated_at: new Date(2000, 0, 5),
          });
          const records = await dataStore.select("people");
          expect(records).toHaveLength(1);

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at.getTime()).toBeCloseTo(Date.now(), -1);
        }),
      );
    });

    describe("when not specified in insert", () => {
      it(
        "sets to the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });
          const records = await dataStore.select("people");
          expect(records).toHaveLength(1);

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at.getTime()).toBeCloseTo(Date.now(), -1);
        }),
      );
    });
  });
});
