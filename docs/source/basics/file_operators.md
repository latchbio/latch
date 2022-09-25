# Operators

The Latch SDK offers several operators for grouping and organizing data.

---

## Joins

We now provide the functions `left_join`, `right_join`, `inner_join`, and `outer_join`, which perform their respective flavors of joins on two dictionaries of data. All joins operate on keys, i.e. objects in both dictionaries with the same key will be joined together. The rules for joining are very straightforward:

* If both items are lists, the lists are concatenated,
* If only one of the items is a list, the other is appended to that list,
* Otherwise, the output is a new list containing both items,

You can read about the differences between these respective joins [here](https://en.wikipedia.org/wiki/Join_(SQL)).

## `group_tuple`

This operator mimics the corresponding construct from NextFlow: The `group_tuple` operator takes in a list of tuples and groups together the elements that share the same key. By default, keys are the first element of the tuple, but users can provide a `key_index` argument to specify which element of the tuple should be used for the key. Example:

```python
>>> channel = [(1,'A'), (1,'B'), (2,'C'), (3, 'B'), (1,'C'), (2, 'A'), (3, 'D')]
>>> group_tuple(channel) # key_index defaults to grouping by the first element (index 0)
[(1, ['A', 'B', 'C']), (2, ['C', 'A']), (3, ['B', 'D'])]
>>> group_tuple(channel, key_index=1) 
[([1, 2], 'A'), ([1, 3], 'B'), ([2, 1], 'C'), ([3], 'D')]
```

## `latch_filter`

This operator runs a filter over a list and returns a new list. The filter can be either a

* A predicate function (i.e. a function that takes in an element of the list and returns a boolean),
* A regular expression, or
* A type.

If using a predicate, `latch_filter` will return a new list containing all the elements for which the predicate is True. If using a regular expression, `latch_filter` will return all string elements of the list that match the regular expression, skipping non-string elements. Finally, if using a type, `latch_filter` will return a list of all elements of the original list that are instances of the type provided.

## `combine`

This operator creates a Cartesian product of the two provided lists, with the option to first group the elements of the list by a certain index and then doing individual products on the groups. Example

```python
>>> c0 = ['hello', 'ciao']
>>> c1 = [1, 2, 3]
>>> combine(c0, c1)
[('hello', 1), ('hello', 2), ('hello', 3), ('ciao', 1), ('ciao', 2), ('ciao', 3)]
```
