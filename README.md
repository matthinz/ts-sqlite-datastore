# ts-sqlite-datastore

Version: 0.0.1

This is a single file, Promise-oriented API for working with a local sqlite
database in Node.js. The idea is that you copy sqlite-datastore.ts into your
project.

## Column types

sqlite represents data using four types:

- `TEXT`
- `BLOB`
- `INTEGER`
- `REAL`

These four map *somewhat* cleanly onto Javascript types. Values can also be
NULL, but we're going to disregard that for now.

The JsTypeForSqliteNativeType helper allows us to convert between sqlite types
and Javascript types. We also optionally allow specifying nullability here.

## Schemas

Using one of the four native types, we can describe a **Column** in a database.

There are a couple of different flavors of column, so we define a few subtypes,
then combine them all into one ColumnSchema type.

(Later we'll want to be able to extract a Javascript type from a Column schema.)

A Table is composed of one or more Columns.

We will need some utility types for this next bit.

We'll want to be able to derive a Javascript type for the records a Table
contains.

When a column has a default value defined, it doesn't need to be specified
on insert. InsertRecordFor<T> returns a Record type for the given table, with
all columns that have default values marked as Optional.

A Schema, then, is a set of Tables.

We want to be able to pull out the Table names in a schema

## Creating a SqliteDatastore

## Inserting records

We want to be able to cover a few different insertion scenarios:

1. Insert a single record
2. Insert multiple records

After we insert records, it should be possible to:

- Get the number of records inserted
- Get the IDs of records that were auto-incremented

However, if the number of records inserted is very large, we might not actually
care about what IDs were generated.

## Selecting records

## Counting records

## Implementation

### Error classes

We'll wrap errors thrown by the sqlite driver with these classes.
