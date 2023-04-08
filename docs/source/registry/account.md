# Account Objects

An `Account` object describes an account on Latch. An `Account` can be instantiated either using the class method `Account.current()` (recommended), or directly using its ID.

When calling `Account.current()`, the returned `Account` object is different depending on the context in which it is run:

- When running in an execution, the returned `Account` corresponds to the workspace in which the execution was run. This means that if User A runs an execution in Workspace B, the returned `Account` is for Team B.
- When running outside of an execution, in e.g. `latch develop`, the returned `Account` corresponds to the setting of `latch workspace` at calling time, defaulting to the user if no setting is found.

`Account`s are lazy, in that they don't perform any network requests without an explicit call to `Account.load()` or to a property getter.

## Instance Methods

The only non-getter method on an `Account` is `Account.load()`. This method, if called, will perform a network request and cache values for each of the `Account`'s properties.

### Property Getters

All property getters have an optional `load_if_missing` boolean argument which, if `True`, will call `Account.load()` if the requested property has not been loaded already. This defaults to `True`.

- `Account.list_projects()` will return a list of `Project` objects, each correspondng to a project within the calling `Account`.
