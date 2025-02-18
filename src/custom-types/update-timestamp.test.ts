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
        updated_at: "update_timestamp",
      },
      primaryKey: "id",
    },
  },
} satisfies Schema;

describe("update_timestamp custom type", () => {
  const NOW = new Date(2025, 1, 17, 12, 13, 14);
  const LATER = new Date(2025, 1, 18, 13, 14, 15);

  beforeAll(() => {
    jest.useFakeTimers({ advanceTimers: true });
  });

  beforeEach(() => {
    jest.setSystemTime(NOW);
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  describe("on insert", () => {
    describe("when value is specified", () => {
      it(
        "throws an InsertError",
        testWithSchema(SCHEMA, async (dataStore) => {
          await expect(
            dataStore.insert("people", {
              name: "foo",
              updated_at: new Date(2000, 0, 5),
            }),
          ).rejects.toThrow(InsertError);
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
          expect(records).toHaveLength(1);

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at.getTime()).toBeCloseTo(Date.now(), -1);
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

          await expect(
            dataStore.update("people", {
              set: { name: "bar", updated_at: new Date(2000, 0, 5) },
            }),
          ).rejects.toThrow(UpdateError);
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

          expect(records).toHaveLength(1);
          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at.getTime() - NOW.getTime()).toBeLessThan(
            1000,
          );

          jest.setSystemTime(LATER);

          await dataStore.update("people", {
            set: { name: "bar" },
          });

          records = await dataStore.select("people");

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(
            records[0].updated_at.getTime() - LATER.getTime(),
          ).toBeLessThan(1000);
        }),
      );
    });
  });
});
