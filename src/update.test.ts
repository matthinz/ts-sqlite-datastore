import { Schema } from "./sqlite-datastore";
import { createDataStore, testWithSchema } from "./test-utils";

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

describe("#update", () => {
  describe("update all records in a table with an object", () => {
    it("updates all records", async () => {
      const [dataStore] = await createDataStore({
        schema: TEST_SCHEMA,
      });

      await dataStore.insert("people", { name: "foo" });
      await dataStore.insert("people", { name: "bar" });

      const result = await dataStore.update("people", {
        set: {
          name: "baz",
        },
      });
      expect(result).toHaveProperty("count", 2);

      const records = await dataStore.select("people");
      expect(records).toEqual([
        { id: 1, name: "baz", birthdate: null },
        { id: 2, name: "baz", birthdate: null },
      ]);
    });
  });

  it("updates some records using where clause", async () => {
    const [dataStore] = await createDataStore({
      schema: TEST_SCHEMA,
    });

    await dataStore.insert("people", { name: "foo" });
    await dataStore.insert("people", { name: "bar" });

    const result = await dataStore.update("people", {
      set: {
        name: "baz",
      },
      where: {
        name: "foo",
      },
    });

    expect(result).toHaveProperty("count", 1);

    const records = await dataStore.select("people");

    expect(records).toEqual([
      { id: 1, name: "baz", birthdate: null },
      { id: 2, name: "bar", birthdate: null },
    ]);
  });

  describe("update_timestamp custom type", () => {
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

    describe("when timestamp is specified in update", () => {
      it(
        "uses the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });
          let records = await dataStore.select("people");
          expect(records).toHaveLength(1);

          const ogUpdatedAt = records[0].updated_at;

          await dataStore.update("people", {
            set: { name: "bar", updated_at: new Date(2000, 0, 5) },
          });

          records = await dataStore.select("people");

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at).not.toEqual(ogUpdatedAt);
          expect(records[0].updated_at.getTime()).toBeCloseTo(Date.now(), -1);
        }),
      );
    });

    describe("when timestamp is not specified in update", () => {
      it(
        "uses the current timestamp",
        testWithSchema(SCHEMA, async (dataStore) => {
          await dataStore.insert("people", {
            name: "foo",
          });
          let records = await dataStore.select("people");
          expect(records).toHaveLength(1);

          const ogUpdatedAt = records[0].updated_at;
          expect(ogUpdatedAt).toBeInstanceOf(Date);
          expect(ogUpdatedAt.getTime()).toBeCloseTo(Date.now(), -1);

          // TODO: Allow injecting "what time is it" into data store
          await new Promise((resolve) => setTimeout(resolve, 100));

          await dataStore.update("people", {
            set: { name: "bar" },
          });

          records = await dataStore.select("people");

          expect(records[0]).toHaveProperty("updated_at");
          expect(records[0].updated_at).toBeInstanceOf(Date);
          expect(records[0].updated_at).not.toEqual(ogUpdatedAt);
          expect(records[0].updated_at.getTime()).toBeCloseTo(Date.now(), -1);
        }),
      );
    });
  });
});
