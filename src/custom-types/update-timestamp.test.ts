import assert from "node:assert";
import { before, describe, it, mock } from "node:test";
import { InsertError, UpdateError, type Schema } from "../sqlite-datastore.ts";
import { testWithSchema } from "../test-utils.ts";

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

describe("update_timestamp custom type", () => {
  const NOW = new Date(2025, 1, 17, 12, 13, 14);
  const LATER = new Date(2025, 1, 18, 13, 14, 15);

  before(() => {
    mock.timers.enable({ apis: ["Date"] });
  });

  describe("on insert", () => {
    describe("when value is specified", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await assert.rejects(
            dataStore.insert("people", {
              name: "foo",
              updated_at: new Date(2000, 0, 5),
            }),
            InsertError,
          );
        }),
      );
    });

    describe("when value is not specified", () => {
      it(
        "sets to the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });
          const records = await dataStore.select("people");
          assert.equal(records.length, 1);

          assert(records[0].updated_at instanceof Date, "updated_at is a Date");
          assert(
            Date.now() - records[0].updated_at.getTime() < 1000,
            "updated_at is within 1 second of now",
          );
        }),
      );
    });
  });

  describe("on update", () => {
    describe("when value is specified", () => {
      it(
        "throws an UpdateError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });

          await assert.rejects(
            dataStore.update("people", {
              set: { name: "bar", updated_at: new Date(2000, 0, 5) },
            }),
            UpdateError,
          );
        }),
      );
    });

    describe("when value is not specified", () => {
      it(
        "uses the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });
          let records = await dataStore.select("people");

          assert.equal(records.length, 1);
          assert(records[0].updated_at instanceof Date);
          assert(
            records[0].updated_at.getTime() - NOW.getTime() < 1000,
            "updated_at is within 1 second of now",
          );

          mock.timers.setTime(Number(LATER));

          await dataStore.update("people", {
            set: { name: "bar" },
          });

          records = await dataStore.select("people");

          assert(records[0].updated_at instanceof Date, "updated_at is a Date");
          assert(
            records[0].updated_at.getTime() - LATER.getTime() < 1000,
            "updated_at is within 1 second of the current time",
          );
        }),
      );
    });
  });
});
