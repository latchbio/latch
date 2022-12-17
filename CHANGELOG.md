<!-- Based off of https://keepachangelog.com/en/1.0.0/ -->

<!--

Types of changes

    *Added* for new features.
    *Changed* for changes in existing functionality.
    *Deprecated* for soon-to-be removed features.
    *Removed* for now removed features.
    *Fixed* for any bug fixes.
    *Security* in case of vulnerabilities.

-->

# Latch SDK Changelog

## 3.0.0 - 2022-12-08

### Added

* The `latch develop` command, and with it an ecosystem supporting local
  development and faster debugging.

### Deprecated

* The commands `latch rm`, `latch mkdir`, and `latch touch`.
* The operators `left_join`, `right_join`, `inner_join`, `outer_join`,
  `group_tuple`, `latch_filter`, and `combine`