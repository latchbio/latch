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
## 2.13.1 - 2023-02-17

### Fixed

* Create latch config file if it does not exist at registration or develop time

### Added

* `latch init`: Docker in Docker template workflow
* `latch init`: Docker base image
* Small, medium, and large tasks run with sysbox runtime allowing the execution of system-level software such as `systemd`, Docker, Kubernetes, K3s, `buildx`, legacy apps, and more.

## 2.13.1 - 2023-02-17

### Fixed

* Add latch/latch_cli/services/init/common to pypi release

## 2.13.0 - 2023-02-17

### Added

* The `latch dockerfile` command which auto-generates a Dockerfile from files in the workflow directory.
* The `latch init` command can use base images with hardware acceleration using the `--cuda` and the `--opencl` flags
* The `latch init` command does not populate the workflow directory with a Dockerfile by default
* The `latch init` command will populate the workflow directory with a Dockerfile if passed `--dockerfile`
* The `latch register` and `latch develop` commands auto-generate a dockerfile from files in the workflow directory if no Dockerfile is present
* Documentation for the auto-generated Dockerfile feature

### Fixed

* Quickstart tutorial is written factually
* Getting started docs are written factually

## 2.12.1 - 2023-02-08

### Added

* `latch develop` documentation updates

### Fixed

* `latch develop` throws error if user does not have rsync installed
* `pip install latch` installs the `watchfiles` package for `latch develop`

## 2.11.1 - 2023-01-25

### Fixed

* LatchDir initialized with local path initialized to Path object fails on upload

## 2.12.0 - 2023-02-06

### Added

* `latch develop` drops users directly into a shell in their docker environment. Local changes in the workflow directory and any subdirectories are automatically synced into the environment. Deleted local files are not deleted in the environment. However, any additions or modifications to files and directories are propagated.

### Removed

* latch develop no longer drops user into REPL with multiple options -- it goes straight to a shell.

## 2.11.1 - 2023-01-25

### Fixed

* LatchDir initialized with local path initialized to Path object fails on upload

## 2.11.0 - 2023-01-20

### Added

* Use best practices in `latch init` templates
*   * `LatchOutputDir` used to indicate output location on Latch
*   * Regex rules used to validate files
*   * Splits tasks into files
*   * Include empty template
*   * Remove yaml metadata from docstring
*   * Use messages in examples
*   * Error handling
*   * Add LICENSE file
*   * Add README file
* Allow user to select template in GUI or pass flag
* Allow user to run `latch init .`

### Fixed

* LatchDir type transformer bug with Annotated types
*   * LatchOutputDir is fixed

## 2.10.0 - 2023-01-14

### Added

* The `latch develop` command, and with it an ecosystem supporting local
  development and faster debugging.
* The `latch cp` command now displays a x number of files out of n indicator
  and displays which stage of the download is going on (network request to get
  presigned urls vs downloading blob data).
* A new error that is thrown when there is an inconsistency between a
  `LatchMetadata` object and its associated workflow's parameters.
* The function `get_secret` which allows users to reference secrets they've
  uploaded to the Latch platform from within a workflow.

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
* The crash reporter now prints stack traces of caught exceptions in the
  correct order
* `latch develop` now throws an error when run on a workflow that hasn't been
  registered yet.
* Reworked how internal configs are stored, eschewing a flat dictionary of API
  endpoints in favor of a nested dataclass. This removes a small class of
  potential mistakes arising from misspelling, and gives the benefit of IDE
  intellisense.
* Made both configs singletons.

## 2.10.1 - 2023-01-18

### Fixed

* Fixed issue with registering libraries containing nested imports used as
    subclasses (eg. `torch`)
