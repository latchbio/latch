# Record Objects

A `Record` object describes a Registry Record, and can be created either from an `Table` via `Table.list_records()` or directly using its ID.

`Record`s are lazy, in that they don't perform any network requests without an explicit call to `Record.load()` or to a property getter.

## Instance Methods

The only non-getter method on a `Record` is `Record.load()`. This method, if called, will perform a network request and cache values for each of the `Record`'s properties.

### Property Getters

All property getters have an optional `load_if_missing` boolean argument which, if `True`, will call `Record.load()` if the requested property has not been loaded already. This defaults to `True`.

- `Record.get_name()` will return the `name` of the calling `Record` as a string.
- `Record.get_values()` will return a dictionary of the calling `Record`'s values. The keys of the dictionary are strings and must be valid column keys of the calling `Record`'s containing `Table`. The values of the dictionary can either be valid python values, or the special values `EmptyCell` or `InvalidValue`.
