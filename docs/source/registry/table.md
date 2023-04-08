# Table Objects

`Table` objects describe Registry Tables. A `Table` object can either be instantiated via a call to `Project.list_tables()` or directly using its ID.

`Table`s are for the most part lazy, in that they don't perform any network requests without an explicit call to `Table.load()` or to a property getter. Two exceptions to this are `Table.list_records()` and `Table.update()`, both of which are discussed below.

## Instance Methods

- `Table.load()`, if called, will perform a network request and cache values for each of the `Table`'s properties.
- `Table.list_records()` will return a generator that yields a paginated dictionary of `Record`s that are present in the calling `Table`. The keys of this dictionary are Record IDs and the values are the corresponding `Record` objects. This function also takes an optional keyword-only `page_size` argument that dictates the size of the returned page. The value of this argument must be a postive integer. If not provided, the default is a page size of 100. Pages are ordered by Record ID, with lower IDs being yielded first.

An example of typical usage of `Table.list_records()` is below.

```python
from latch.registry.table import Table

tbl = Table(id="1234")

for page in tbl.list_records():
    for record_id, record in page.items():
        # do stuff with the Record `record`.
        ...

```

Unlike the rest of this API, `Table.list_records()` will always perform a network request for each returned page.

### Property Getters

All property getters have an optional `load_if_missing` boolean argument which, if `True`, will call `Account.load()` if the requested property has not been loaded already. This defaults to `True`.

- `Table.get_display_name()` will return the `display_name` of the calling `Table` as a string
- `Table.get_columns()` will return a dictionary containing the columns of the calling `Table`. The keys of the dictionary are column names, and its values are `Column` objects. `Column` is a convenience dataclass with the properties
  - `Column.key`: the key of the column.
  - `Column.type`: the (python) type of the column.
  - `Column.upstream_type`: similar to `Column.type`. However, this is a dataclass which contains an internal representation of the column's data type, and should not be accessed or modified directly.

### Updater

A `Table` can be modified by using the `Table.update()` function. `Table.update()` returns a context manager (and hence must be called using `with` syntax) with the following methods:

- `upsert_record(record_name: str, column_data: Dict[str, Any])` will either (up)date or in(sert) a record with name `record_name` with the column values prescribed in `column_data`. Each key of `column_data` must be a valid column key (meaning that there must be a column in the calling `Table` with the same key), and the value corresponding to that key must be same type as the column (meaning that it is an instance of the column's (python) type).

#### Planned Methods (Not Implemented Yet)

- `delete_record(record_name: str)`
  <!-- should we use Column objects here? -->
- `upsert_column(column_name: str, type: Type)`
- `delete_column(column_name: str)`

The following is an example for how to update a `Table`.

```python
from latch.registry.table import Table

t = Table(id="1234")

with t.update() as updater:
    updater.upsert_record(
        "record 1",
        {
            "Size": 10,
        }
    )
    updater.upsert_record(
        "record 2",
        {
            "Size": 15,
        }
    )
```

The code above will upsert two records, called `record 1` and `record 2`, with the provided values for the column `Size`.

When using an updater, no network requests are made until the end of the `with` block. This mimics transactions in relational database systems, and has several similar behaviors, namely that

- If any exception is thrown inside the `with` block, none of the updates made inside the `with` block will be sent over the network, so no changes will be made to the `Table`.
- All updates are made at once in a single network request at the end of the `with` block. This significantly boosts performance when a large amount of updates are made at once.
