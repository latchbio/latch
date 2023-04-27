<!-- Based off of https://keepachangelog.com/en/1.0.0/ -->

<!--

Types of changes

    *Added* for new features.
    *Changed* for changes in existing functionality.
    *Dependencies* for changes in dependencies.
    *Deprecated* for soon-to-be removed features.
    *Removed* for now removed features.
    *Fixed* for any bug fixes.
    *Security* in case of vulnerabilities.

-->

# Latch SDK Changelog

## 2.19.3 - 2023-04-26

### Fixed

- Tasks stuck initializing on nodes that ran multiple tasks before

## 2.19.2 - 2023-04-24

### Dependencies

* Upgrades lytekit to `0.14.10`

## 2.19.1 - 2023-04-21

### Changed

- `latch cp` now has a progress bar for downloads (previously they only showed for uploads)

## 2.19.0 - 2023-04-14

### Added

- Functions for creating/deleting Projects and Tables, and creating Table columns in the Registry API

## 2.18.3 - 2023-04-12

### Fixed

- Registry table updates should work with columns that have spaces in names

## 2.18.2 - 2023-04-11

### Changed

- Improved Registry API `__str__` and `__repr__`

## 2.18.1 - 2023-04-08

### Fixed

- Registry API should work in an running asyncio event loop

## 2.18.0 - 2023-04-08

### Added

- A new API for interacting with Latch Registry

### Dependencies

- Added `gql` and `aiohttp` as dependencies

## 2.17.2 - 2023-04-07

### Fixed

- Prevent `latch cp` from hitting the s3 part limits which causes large
  file uploads to fail

## 2.17.1 - 2023-04-05

### Fixed

- Switched a dictionary union from `|=` notation to a manual for-loop to
  maintain support for Python 3.8.

## 2.17.0 - 2023-04-01

### Added

- Option to disable default bulk execution mechanism when an alternative (e.g. using a samplesheet parameter) is supported by the workflow itself

## 2.16.0 - 2023-04-01

### Added

- Verified Trim Galore adapter trimming workflow stub

## 2.15.1 - 2023-03-30

### Fixed

* Custom tasks with less than 32 cores receiving incorrect toleration

## 2.15.0 - 2023-03-29

### Added

* Verified MAFFT alignment workflow stub

## 2.14.2 - 2023-03-24

### Fixed

* Parameter flow forks should preserve the order of branches

## 2.14.1 - 2023-03-21

### Fixed

* `latch` commands which require authentication prompt user for token when no browser is present on machine

## 2.14.0 - 2023.03-18

### Added

* SampleSheet metadata to `LatchParameter` -- allows for importing samples from Registry

## 2.13.5 - 2023-02-21

### Fixed

* `latch register` ssh-keygen bug when `.latch` folder does not exist

## 2.13.4 - 2023-02-21

### Dependencies

* Upgrades lytekit to `0.14.9`

## 2.13.3 - 2023-02-21

### Fixed

* Docker template uses correct base image

## 2.13.2 - 2023-02-21

### Fixed

* Internal state file should be automatically created when running `latch register` and `latch develop`

### Added

* `latch init`: Docker in Docker template workflow
* `latch init`: Docker base image
* Small, medium, and large tasks use the [Sysbox runtime](https://github.com/nestybox/sysbox) to run Docker and other system software within task containers

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
