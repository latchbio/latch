# Registry Projects

A `Project` object describes a Registry Project, and can be created either from an `Account` via `Account.list_projects()` or directly using its ID.

`Project`s are lazy, in that they don't perform any network requests without an explicit call to `Project.load()` or to a property getter.

## Instance Methods

The only non-getter method on a `Project` is `Project.load()`. This method, if called, will perform a network request and cache values for each of the `Project`'s properties.

### Property Getters

All property getters have an optional `load_if_missing` boolean argument which, if `True`, will call `Project.load()` if the requested property has not been loaded already. This defaults to `True`.

- `Project.get_display_name()` will return the `display_name` of the calling `Project` as a string.
- `Project.list_tables()` will return a list of `Table` objects, each corresponding to a table within the calling `Project`.

### Updater

A `Project` can be modified by using the `Project.update()` function. `Project.update()` returns a context manager (and hence must be called using `with` syntax) with the following methods:

- `upsert_table(name: str)` will create a table with name `name`.
- `delete_table(id: str)` will delete the table with id `id`. If no such table exists, the method call will be a noop.
