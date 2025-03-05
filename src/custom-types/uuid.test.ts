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

          expect(actual).toHaveLength(1);

          const { id } = actual[0] as any;

          expect(id).toMatch(
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

          expect(actual).toEqual([
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

          expect(records).toHaveLength(1);
          expect(records[0]).toHaveProperty("id");
          expect(records[0].id).toEqual("EAAE1BB6-1AD8-4952-8AAA-C0AA5B60AEA0");
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

          await expect(
            dataStore.update("people", { set: { id: "not a uuid" } }),
          ).rejects.toThrow(InvalidUUIDError);
        }),
      );
    });
  });
});
