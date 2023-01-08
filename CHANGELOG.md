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
* The `latch cp` command now displays a x number of files out of n indicator
  and displays which stage of the download is going on (network request to get
  presigned urls vs downloading blob data).
* A new error that is thrown when there is an inconsistency between a
  `LatchMetadata` object and its associated workflow's parameters.

### Deprecated

* The commands `latch rm`, `latch mkdir`, and `latch touch`.
* The operators `left_join`, `right_join`, `inner_join`, `outer_join`,
  `group_tuple`, `latch_filter`, and `combine`

### Removed

* Removed a broken SDK test (launching CRISPResso2)

### Fixed

* `requests` library given higher version lower bound to fix warning with one of its dependencies
* `lytekit` version updated to
  * pin `numpy` to version `1.22` (breaking changes in later versions of this library)
  * have better behavior when downloading directories during local development
  * force retry on connection closed on file uploads (POST requests  more generally)
* `latch get-params` will escape class attribute names (representation of Enum
  type) if they are python keywords
* `latch preview` now requires a directory argument instead of a workflow name
  argument, and now behaves consistently with regular parameter interface
  generation.
