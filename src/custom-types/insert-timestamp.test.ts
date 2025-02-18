import { InsertError, Schema, UpdateError } from "../sqlite-datastore";
import { testWithSchema } from "../test-utils";

const SCHEMA = {
  tables: {
    people: {
      columns: {
        id: {
          type: "INTEGER",
          autoIncrement: true,
        },
        name: "TEXT",
        created_at: "insert_timestamp",
      },
      primaryKey: "id",
    },
  },
} satisfies Schema;

describe("insert_timestamp custom type", () => {
  describe("on insert", () => {
    describe("when no value provided for created_at", () => {
      it(
        "uses the current date/time",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", { name: "foo" });

          const records = await dataStore.select("people");

          expect(records).toHaveLength(1);

          expect(records[0]).toHaveProperty("created_at");
          expect(records[0].created_at).toBeInstanceOf(Date);
          expect(records[0].created_at).not.toBeNaN();

          expect(Date.now() - records[0].created_at.getTime()).toBeLessThan(
            1000,
          );
        }),
      );
    });

    describe("when Date provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });

    describe("when valid ISO date string provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore, db) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });

    describe("when invalid value provided for created_at", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              created_at: new Date(),
            }),
          ).rejects.toThrow(InsertError);
        }),
      );
    });
  });

  describe("on update", () => {
    describe("when value provided for created_at", () => {
      it(
        "throws an UpdateError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });

          expect(
            dataStore.update("people", {
              set: {
                created_at: new Date(),
              },
            }),
          ).rejects.toThrow(UpdateError);
        }),
      );
    });
  });
});
