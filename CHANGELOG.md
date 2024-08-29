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

## 2.51.2 - 2024-08-27

### Fixed

* Bump lytekit version to fix download of empty files

## 2.51.1 - 2024-08-21

### Fixed

* Remove caching from `tinyrequests` module

## 2.51.0 - 2024-08-16

### Added

* New task annotations for V100 GPU enabled nodes
  * `@v100_x1_task`: 1 V100 GPU
  * `@v100_x4_task`: 4 V100 GPUs
  * `@v100_x8_task`: 8 V100 GPUs

## 2.50.6 - 2024-08-15

### Added

* The `--chunk-size-mib` to `latch cp` which allows users to configure the chunk size of uploads

### Changed

* Lower the default chunk size for `latch cp` to `64 MiB`
* `latch cp` now reuses connections for speed

## 2.50.5 - 2024-08-12

### Fixed

* Fix a bug where GPU template selection wouldn't happen correctly in `latch init`

## 2.50.4 - 2024-08-09

### Changed

* Nextflow
  - Update `nextflow` base image to version `v1.1.7`

## 2.50.3 - 2024-08-08

### Fixed

* Nextflow
  - Normalize result paths before publishing results

## 2.50.2 - 2024-08-07

### Dependencies

* lytekit `0.15.10` -> `0.15.11` which parallelizes file downloads from Latch Data

## 2.50.1 - 2024-08-05

### Added

* Nextflow
  - Report workdir size on workflow completion

## 2.50.0 - 2024-08-02

### Added

* Add git commit information to version string if exists

## 2.49.10 - 2024-08-02

### Fixed

* Pin `cryptography` to remove warning messages when importing paramiko

## 2.49.9 - 2024-08-01

### Fixed

* Better error messages when using `latch exec` on a task that is not yet running

## 2.49.8 - 2024-07-29

### Fixed

* Nextflow
  - Indentation error in samplesheet constructor function call

## 2.49.7 - 2024-07-26

### Changed

* Nextflow
  - Cleanup Nextflow logging
  - Update `nextflow` base image to version `v1.1.6`

## 2.49.6 - 2024-07-23

### Added

* Ability to publish results to be displayed on executions `Results` page

## 2.49.5 - 2024-07-20

### Added

* Add `--cores` parameter to `latch cp`

## 2.49.4 - 2024-07-19

### Added

* `allowed_tables` argument to `LatchParameter` to constrain the set of registry tables selectable from a samplesheet parameter

## 2.49.3 - 2024-07-19

### Added

* Ability to override the target workspace for registration using the `LATCH_WORKSPACE` env variable.

## 2.49.2 - 2024-07-18

### Added

* Nextflow
  - Make EFS storage period configurable

## 2.49.1 - 2024-07-17

### Fixed

* `latch cp` bug where directories containing symlinks would hang and not complete uploading

## 2.49.0 - 2024-07-17

### Changed

* Nextflow
  - Update `nextflow` base image to version `v1.1.5`

## 2.48.9 - 2024-07-16

### Added

* Nextflow
  - Make execution profile configurable via UI

## 2.48.8 - 2024-07-12

### Changed

* Nextflow
  - Update `nextflow` base image to version `v1.1.4`

## 2.48.7 - 2024-07-11

### Fixed

* Sync
  - Update objects with no ingress event

## 2.48.4 - 2024-07-10

### Added

* Sync
  - Add option for number of cores

## 2.48.3 - 2024-07-08

### Added

* Snakemake
  - Don't throw an exception on `subprocess.CalledProcessError`, `sys.exit(1)` instead to avoid polluting stdout with a traceback

### Dependencies

* lytekit `0.15.9` -> `0.15.10` which removes unnecessary prints in exception wrappers

## 2.48.2 - 2024-07-06

### Added

* Nextflow
  - ignore `.git` directory when copying /root to workdir


## 2.48.1 - 2024-07-05

### Added

* Nextflow
  - use nextflow dockerfile instead of downloading the binary directly

## 2.48.0 - 2024-07-03

### Added

* Nextflow
  - bump nf runtime version to 1.1.3

## 2.47.9 - 2024-07-02

### Dependencies

* Use `lytekit==0.15.9` which includes retries for file downloads

## 2.47.8 - 2024-06-29

### Fixed

* latch sync upload in parallel
* latch sync missing normalize_path call
* latch sync cosmetic path display extra slash

## 2.47.7 - 2024-06-28

### Fixed

* Timeout in tinyrequestst

## 2.47.6 - 2024-06-27

### Fixed

* Snakemake + Nextflow
  - fix about page file validation

## 2.47.5 - 2024-06-26

### Fixed

* Nextflow
  - remove `storage_gib` parameter from pvc provisioning request

## 2.47.4 - 2024-06-25

### Added

* Nextflow
  - bump nf runtime version to 1.1.1

## 2.47.3 - 2024-06-25

### Added

* Nextflow
  - bump nf runtime version to 1.1.0

## 2.47.2 - 2024-06-24

### Added

* Snakemake
  - expose underlying Latch paths in config object

## 2.47.1 - 2024-06-22

### Added

* Nextflow version 1.0.11

### Fixed

* Snakemake
  - fix dataclass name collisions when generating metadata for Snakemake workflows

## 2.47.0 - 2024-06-20

### Added

* Nextflow version 1.0.10

## 2.46.9 - 2024-06-20

### Fixed

* Snakemake
  + fix metadata_root param for Snakemake workflows

## 2.46.8 - 2024-06-19

### Added

* Nextflow
  + Support for relaunching from failed task using `-resume` flag

## 2.46.7 -- 2024-06-19

### Fixed

* Nextflow
  + Bump Nextflow version to 1.0.8 to add retries on failure

## 2.46.6 - 2024-06-14

### Added

* Nextflow + Snakemake
  + Add `--metadata-root` option to `latch register`

### Fixed

* Nextflow
  + Bump Nextflow version to fix releasing workflow to different workspace

## 2.46.5 - 2024-06-13

### Added

* Nextflow
  + Command to upgrade nextflow version

## 2.46.4 - 2024-06-13

### Fixed

* Nextflow
  + Bump nextflow version to v1.0.4 -- fixes large file uploads

## 2.46.3 - 2024-06-12

### Fixed

* Nextflow
  + Upload .nextflow.log on success and failure
  + Add tolerations to nextflow runtime task
  + Bump nextflow version to v1.0.2

## 2.46.2 - 2024-06-11

### Added

* New error messages when trying to access registry projects/tables/records that don't exist.

## 2.46.1 - 2024-06-10

### Removed

* Nextflow
  + Remove /finalize call at end of workflow


## 2.46.0 - 2024-06-10

### Added

* Nextflow
  + Bump version from v1.0.0 to v1.0.1 -- resolves upload data corruption

## 2.45.8 - 2024-06-08

### Fixed

* Nextflow
  + Fix default generation for parameters with type: `typing.Optional[LatchFile,LatchDir]`

## 2.45.7 - 2024-06-07

### Added

* Nextflow
  + Support for About page
  + Pin Nextflow version to latch version (deprecate --redownload-dependencies)

### Added

* Snakemake
  * Support for adding About page content from a markdown file

## 2.45.6 - 2024-06-04

### Added

* Nextflow
  + Support for generating metadata from `nextflow_schema.json`
  + Command to generate Nextflow entrypoint without registering

### Fixed

+ Make `LatchRule` class a dataclass

## 2.45.5 - 2024-06-03

### Added

+ Support for uploading Nextflow workflows


## 2.45.4 - 2024-05-29

### Changed

+ `TableUpdater.upsert_record` no longer does file path resolution, and instead
  defers that operation to when the update commits. This drastically speeds up
  registry table updates to blob-type columns.

## 2.45.3 - 2024-05-28

### Added

+ Support selecting organization workspaces with `latch workspace`

## 2.45.2 - 2024-05-20

### Dependencies

+ Bump docker version to fix <https://github.com/docker/docker-py/issues/3256>

## 2.45.0 - 2024-05-20

### Fixed

+ Latch resource limit on mem-512-spot should be 485 Gi instead of 490 Gi

## 2.44.0 - 2024-05-19

### Fixed

+ Accomodates the migration of personal workspaces to team workspaces.

## 2.43.3 - 2024-05-16

### Fixed

* Snakemake
  + remove conversion of primitive types to string for input `params`
  + add support for output directories

## 2.43.2 - 2024-05-16

### Fixed

+ Circular import when importing latch constants
* Snakemake
  + support for running python scripts inside containers

## 2.43.1 - 2024-05-15

### Fixed

+ Cleanup toleration assignment

### Added

+ Support up to 126 cores and 975 GiB of memory in custom tasks

## 2.42.1 - 2024-05-08

### Fixed

+ Workflow identifier should be immutable per `latch init`
+ Latch workspace does not display selected workspace when authenticated with workspace token
+ Workspace id determination failing when authenticated with workspace token

## 2.42.0 - 2024-05-02

+ Adding support for GCP mount paths

## 2.41.3 - 2024-04-29

### Fixed

+ Allow the use of `**kwargs` in dynamic resource allocation functions

## 2.41.2 - 2024-04-23

### Fixed

* Snakemake
  + Namespace Snakemake execution outputs by execution name

## 2.41.1 - 2024-04-22

### Fixed

* Snakemake
  + Add support for using directories as inputs to rules


## 2.41.0 - 2024-04-19

### Added

* `--wf-version` flag to `develop` to use a container for a specific workflow version
* `--disable-sync` flag to `develop` to disable automatic syncing of local files. Useful for inspecting workflow code from a past version without overwriting with divergent local changes.

## 2.40.6 - 2024-04-12

### Fixed

* Actually fix the issue where functions that use `current_workspace` would break if called from an execution

## 2.40.5 - 2024-04-09

### Fixed
* bump flytekit version to 0.15.6 which fixes retries in dynamic resource allocation for map tasks

## 2.40.4 - 2024-04-09

### Fixed

* Issue where functions that use `current_workspace` would break if called from an execution
* Issue where CLI commands that required an auth method would not print the correct error message if no auth was present

## 2.40.3 - 2024-04-09

* bump flytekit version to 0.15.5

## 2.40.2 - 2024-04-09

### Fixed

* latch register failing when using launchplans with file or directory types

## 2.40.1 - 2024-04-06

### Added

* add support for allocating task resources at runtime

## 2.40.0 - 2024-04-05

### Fixed

* latch commands default to the user's default workspace instead of personal workspace

## 2.39.7 - 2024-04-03

### Documentation

* latch cp
  + Commandline docstring states recursive default behavior.

## 2.39.6 - 2024-04-02

### Fixed

* latch sync
  + prints error when case other than local -> remote is attempted


## 2.39.4 - 2024-03-11

### Added

* Snakemake
  + Add `container_args` field in `EnvironmentConfig`

## 2.39.3 - 2024-03-07

### Fixed

* Snakemake
  + fix bug where two rules with the same input parameter name causes JIT step to fail


## 2.39.3 - 2024-03-05

### Fixed

* Fix bug where temporary SSH keys were getting added but not removed from the SSH Agent during workflow registration

## 2.39.2 - 2024-03-01

### Fixed

* Fix bug `LPath` resolves to parent node if path does not exist

### Deprecated

* `latch mkdir` command (replaced with `latch mkdirp`)
* `latch rm` command (replaced with `latch rmr`)
* `latch touch`
* `latch open`

## 2.39.1 - 2024-02-27

### Added

* add `LPath` support for uploading and copying to non-existent directories
* clear `LPath` cache after modification operations such as `rm` and `upload`

### Fixed

* skip symlinks that point to non-existent files when running `latch cp`

## 2.39.0 - 2024-02-21

### Added

* add `LPath` implementation
* add support for registering workflows without Latch metadata

## 2.38.9 - 2024-02-14

### Added

* Snakemake
  + add support for optional input parameters

## 2.38.8 - 2024-01-26

### Fixed

* Snakemake
  + fix bug in `ruleorder` directive caused by `block_content` monkey patch returning None

## 2.38.7 - 2024-01-26

### Fixed

* Snakemake
  + warn instead of error when config parameter type cannot be parsed in generate-metadata command

## 2.38.6 - 2024-01-26

### Added

* Snakemake
  + support for config files with nested file paths
  + GPU support for non-container tasks
  + Add `cores` field to SnakemakeMetadata object

## 2.38.5 - 2024-01-22

### Fixed

* Broken default in snakemake metadata which prevented registration of any workflow

## 2.38.4 - 2024-01-20

### Added

* Snakemake
  + update defaults for conda + containers to `False`

## 2.38.3 - 2024-01-19

### Added

* Snakemake
  + support for pulling images from private container registries
  + add config field for running tasks in conda and container environments

### Fixed

* Snakemake
  + fix regression in 2.38.2 that caused failure to resolve upstream nodes for target files

## 2.38.2 - 2024-01-17

### Added

* Snakemake
  + support for per-task containers in Snakemake workflow using the `container` directive

### Fixed

* Snakemake
  + add `_jit_register` suffix when resolving Snakemake workflow name for `latch develop`
  + use `variable_name_for_value` instead of `variable_name_for_file` when resolving upstream jobs for target files

## 2.38.1 - 2024-01-15

### Added

* The `--open` option to `latch register`, which if passed in, will open the workflow in the browser after successful registration

### Fixed

* Snakemake
  + remove `.latch` directory copy from Dockerfile generation to avoid unexpected file overrides
  + limit pulp package version to < 2.8 to fix snakemake import failure

## 2.38.0 - 2024-01-13

### Added

* The `latch exec` command to spawn a shell inside a running task.

## 2.37.1 - 2024-01-08

### Added

* Snakemake
  + support for `latch develop` for JIT workflow

### Fixed

* Snakemake
  + user input config overrides default config instead of merging
  + JIT workflow fails if the same keyword is used to define two different input parameters
  + fail to serialize `snakemake_data` when any params are defined as `pathlib.Path`

## 2.37.0 - 2023-12-11

### Added

* `rename_current_execution` function which allows programmatic execution renaming.

## 2.36.11 - 2023-11-28

### Added

* Snakemake
  + ability to cache snakemake tasks using the `--cache-tasks` option with `latch register`

### Changed

* Minor aesthetic enhancements to `latch workspace`
  + the currently active workspace is now marked
  + the current selection is marked with a `>` for enhanced readability on terminals with limited color support

## 2.36.10 - 2023-11-17

### Fixed

* Snakemake
  + bug in 2.36.9 where output directories would still fail to upload because of a missing `pathlib.Path` -> `str` conversion.

## 2.36.9 - 2023-11-16

### Fixed

* Snakemake
  + bug where a snakemake task would fail to upload output directories

## 2.36.8 - 2023-11-14

### Fixed

* Snakemake
  + bug where a snakemake workflow would only run successfully for the user who registered it

## 2.36.7 - 2023-11-13

### Added

* Snakemake
  + added best effort display name parsing for `generate-metadata`
  + tasks now upload their intermediate outputs for better debugging

### Fixed

* Snakemake
  + bug where `update_mapping` would iterate over the entirety of `/root`

## 2.36.6 - 2023-11-09

### Added

* Added ability to skip version check using an env variable

## 2.36.5 - 2023-11-08

### Fixed

* Bug in `latch login` where not having a token would prevent token generation

## 2.36.4 - 2023-10-25

### Added

* Added ability to get a pandas Dataframe from a registry table.
* Added `Multiselect` to `LatchAppearance`
* Snakemake
  + fixed case where config values would not be populated correctly in the JIT workflow

### Changed

* Better error messaging for both `latch cp` and `latch mv`.

## 2.36.3 - 2023-10-25

### Fixed

* Bug where Python 3.8 clients would crash due to a broken type annotation

## 2.36.2 - 2023-10-25

### Changed

* Snakemake
  + Log files are now marked as outputs - this enables rules to use logs of previous rules as inputs

## 2.36.1 - 2023-10-24

### Changed

* `latch mv` now supports glob patterns (with the same restrictions as `latch cp`)

## 2.36.0 - 2023-10-23

### Added

* `latch.registry.record.Record.get_table_id` method for querying the ID of the table containing a given registry record
* `latch.registry.table.Table.get_project_id` method for querying the ID of the project containing a given registry table

## 2.35.0 - 2023-10-21

### Added

* Snakemake
  + Remote register support
  + `download` field for file inputs
  + `config` field for file inputs
  + Blanket support for parameters of any type via the `SnakemakeParameter` class
  + Support for generating a `latch_metadata` directory from a `config.yaml` with `latch generate-metadata`
  + Support for default values for parameters

### Changed

* Snakemake
  + JIT register step no longer downloads input files by default
  + `latch_metadata` should now be a module (directory containing an `__init__.py` file), as opposed to just being a file

## 2.34.0 - 2023-10-04

### Added

* Snakemake
  + `directory` modifier for input / outputs
  + Support `temp` by removing from compiled rules. All files / directories are
  temporary because they are deleted at the end of each job on Latch.
  + `multiext` output modifier
  + `report` output modifier
  + `params` in rules

### Fixed

* Snakemake
  + Replace skipped rules with `Ellipsis`. Supports rules nested in conditionals where previously an empty block was produced.
  + Patched parser to generate compiled code for `workflow.include` calls Compiled workflow.include should carry `print_compilation` keyword (snakemake/snakemake#2469)
  + Detect use of `conda` keyword and install in image. This effectively supports wrapper/conda keywords.
  + `Iterable, Generator` cause issues as type hints when imported from `collections.abc` rather than `typing`

## 2.33.0 - 2023-09-29

### Added

* Add `latch sync` for synchronization from local to remote directories that only uploads modified content

## 2.32.8 - 2023-09-07

### Fixed

* Snakemake:
    - Better errors if `Snakefile` or `latch_metadata.py` file missing
    - Correct issues with snakemake example project

## 2.32.7 - 2023-09-07

### Fixed

* Snakemake:
    - `--snakemake` for `latch dockerfile` command to generate `Dockerfile` with
    necessary instructions

    - Snakemake example for `latch init`

## 2.32.6 - 2023-09-07

### Fixed

* A bug in `latch develop` where images for newly registered workflows could not be found.

## 2.32.5 - 2023-08-28

### Fixed

* Snakemake:
  + Ignore global ruleorder directive
  + Ignore temporary condition on output values

## 2.32.4 - 2023-08-28

### Fixed

* Fixed a bug in `latch ls` where `datetime.isoformat` was called on strings with timestamps (which is not supported on python < 3.11)

## 2.32.3 - 2023-08-26

### Fixed

* Snakemake issues
  + bounded snakemake versions to prevent compatibility issues that arise in later versions
  + small bugs in list json encoder
  + mishandled http issues.

## 2.32.2 - 2023-08-25

### Fixed

* Fixed `latch ls` to work with all latch URLs
* Fixed autocomplete bug where no completion results would be returned for longer paths

## 2.32.1 - 2023-08-24

### Fixed

* Corrected `dataclass` import and removed `multiprocessing` logging from `latch cp`.

## 2.32.0 - 2023-08-23

### Changed

* `latch cp` can now handle directories with up to `50k` objects

### Fixed

* Various vestigial bugs in `latch develop` that blocked certain users

## 2.31.1 - 2023-08-08

### Changed

* Any CLI command will now display a message if no authentication token is found

## 2.31.0 - 2023-07-28

### Changed

* `latch stop-pod` renamed to `latch pods stop`

## 2.30.0 - 2023-07-27

### Added

* Support for python 3.11

### Dependencies

* pinned `lytekit` to `v0.15.2` to remove numpy + pandas + pyarrow dependencies
* pinned `lytekitplugins-pods` to `v0.6.1` to remove dependency on numpy
* pinned `latch-sdk-gql` to `0.0.6` which supports 3.11
* pinned `latch-sdk-config` to `0.0.4` which supports 3.11

## 2.29.0 - 2023-07-26

### Added

* `stop-pod` command to the CLI. Allows the user to stop a pod in which the CLI resides or to stop a pod using its id.

## 2.28.0 - 2023-07-25

### Added

* Tasks explicitly request ephemeral storage
* `custom_task` and `custom_memory_optimized_task` allow selecting storage size
* `custom_memory_optimized_task` functionality merged into `custom_task`

### Deprecated

* `custom_memory_optimized_task`

## 2.27.4 - 2023-07-18

### Changed

* changed beta register implementation

## 2.27.3 - 2023-07-18

### Dependencies

* pinned `lytekit` to `v0.14.15` to bring in `marshmallow-enum` as a dependency.

## 2.27.2 - 2023-07-17

### Fixed

* fixed bug where `LatchFile`s/`LatchDir`s would provide `file://` URIs instead of Unix paths, which was causing errors in, e.g., calls to `open()`.

## 2.27.1 - 2023-07-15

### Fixed

* fixed bug where `LatchFile`s/`LatchDir`s wouldn't respect the workspace selected using `latch workspace`.

## 2.27.0 - 2023-07-15

### Added

* Added `.iterdir()` method to `LatchDir` to iterate through subdirectories

## 2.26.2 - 2023-07-11

### Fixed

* Fix unclosed file in `lytekit` upload code

## 2.26.1 - 2023-07-10

### Fixed

* LatchFiles accessed through registry are downloaded to a file with the same name as the file on latch

## 2.26.0 - 2023-07-07

### Changed

* Gated `latch develop` resource selection behind an environment variable due to its slow performance

### Dependencies

* Added back several dependencies to allow the old `latch develop` infrastructure to work properly.

## 2.25.2 - 2023-07-05

### Fixed

* Dockerfile generation uses `\` escaping: fixes bug in Conda and R template

## 2.25.1 - 2023-06-28

### Dependencies

* Upgraded `paramiko` dependency to `>=3.2.0` which fixes a `PKey` issue.

## 2.25.0 - 2023-06-28

### Changed

* `latch register` experimental features (2.24.xx) are now gated behind an environment variable, and by default we use the old (pre 2.24) register backend.

## 2.24.12 - 2023-06-27

### Dependencies

* Upgraded lytekit to version 0.14.13 to support uploading files up to 5 TiB from within a task

## 2.24.11 - 2023-06-27

### Fixed

* Internal bug in `latch register` which caused an API call to be made when not necessary, resulting in an irrelevant exception being thrown

## 2.24.10 - 2023-06-27

### Fixed

* Bug in `latch cp` upload path where URLs would be generated but files would not be uploaded

## 2.24.9 - 2023-06-27

### Changed

* Added client side rate limiting to `latch cp` upload API calls so as to not throttle our backend.

## 2.24.8 - 2023-06-27

### Fixed

* `latch register` provision timeout bug
* catch `KeyboardInterrupt`s during register provisioning
* updated `latch-base` image to fix docker-in-docker workflows which use the host machine's network interface

## 2.24.7 - 2023-06-26

### Fixed

* `latch develop` and `latch register` provision timeout increased to 30 minutes

## 2.24.6 - 2023-06-26

### Fixed

* `latch develop` and `latch register` SSH connections timeout on inactivity

## 2.24.5 - 2023-06-26

### Fixed

* Bug in `latch develop` where rsync would continually flood stdout with requests to confirm host key authenticity

## 2.24.3 - 2023-06-24

### Added

* Rename `--no-glob` option shorthand to `-G` for `latch cp`

### Fixed

* Bug in `latch register` where SSH connections were going stale

## 2.24.0 - 2023-06-23

### Added

* Glob support for latch cp
* latch cp autocomplete
* ability to choose task size in latch develop

### Changed

* Backend implementations of latch register and latch develop

## 2.23.5 - 2023-06-19

### Fixed

* Fix limits and imports

## 2.23.3 - 2023-06-19

### Added

* Memory optimized task type

## 2.23.2 - 2023-06-14

### Fixed

* Template generation bug in empty wfs

## 2.23.1 - 2023-06-12

### Fixed

* NFCore template using wrong Latch version

## 2.22.5 - 2023-06-10

### Added

* When a CLI command fails, metadata (`latch` version, current python, os info, etc) is printed.
* There is now a prompt on failure to generate a crash report. Previously reports were generated automatically which was slow and sometimes error-prone.

### Fixed

* Bugs that broke support for Python 3.8 users:
  + Fixed imports of `functools.cache`
  + Fixed `with` statements with multiple contexts

### Dependencies

* Removed unused packages
  + `awscli`
  + `uvloop`
  + `prompt-toolkit`

## 2.22.4 - 2023-06-08

### Fixed

* `catalogMultiCreateExperiments` instead of `catalogMultiUpsertExperiments` in registry API

## 2.22.3 - 2023-06-08

### Fixed

* `catalogMultiCreateProjects` instead of `catalogMultiUpsertProjects` in registry API

## 2.22.2 - 2023-06-08

### Fixed

* `workspace_id` failing when file is empty

## 2.22.1 - 2023-06-05

### Fixed

* `latch cp` failing when uploading a file into a directory without specifying the resulting filename.

## 2.22.0 - 2023-05-31

### Fixed

* `latch cp` occasionally throwing an error when finalizing uploads for directories.

### Added

* `latch cp` now supports remote -> remote copying (i.e. both source and destination are remote paths). This enables copying files across workspaces
* `latch mv` for moving remote files.

## 2.21.7 - 2023-05-29

### Fixed

* Semver violation related to removed `__init__.py` files. These will happen again in the future but a proper major release will be created, communicated, and marketed.

## 2.21.6 - 2023-05-29

### Dependencies

* Upgraded dependency `lytekit` to version `0.14.11`.

## 2.21.5 - 2023-05-29

### Fixed

* More imports in docker/NF-core template workflows have been updated to reflect the import changes outlined in the previous version.

## 2.21.4 - 2023-05-29

### Fixed

* Imports in docker/NF-core template workflows have been updated to reflect the import changes outlined in the previous version.

## 2.21.3 - 2023-05-26

### Added

* NFCore example workflow

### Changed

* Replace docker example workflow with blastp
* Docker image selection when creating an empty workflow
* Workflow Name, Author Name prompts when creating an empty workflow

* `latch cp` has been rewritten and now allows for latch paths of the form
  + `latch:///a/b/c`
  + `latch://xxx.account/a/b/c` where `xxx` is the account ID
  + `latch://shared.xxx.account/a/b/c`
  + `latch://shared/a/b/c`
  + `latch://mount/a/b/c`
  + `latch://xxx.node` where `xxx` is the data ID (viewable in Latch Console)

### Removed

* Unnecessary imports in `__init__.py` files have been removed. Statements like `from latch.types import LatchFile` will no longer work, and such objects should be imported from their defining files instead (in this example, the correct import is `from latch.types.file import LatchFile`)

## 2.19.11 - 2023-05-16

### Fixed

* Revert an undocumented change that caused custom task settings to not work

## 2.19.10 - 2023-05-16

### Fixed

* Typo in `latch init` R template

## 2.19.9 - 2023-05-12

### Fixed

* `latch cp` should automatically detect file content type

## 2.19.8 - 2023-05-11

### Fixed

* Registry API crashes when resolving paths if `~/.latch/workspace` does not exist

## 2.19.7 - 2023-05-09

### Added

* `latch register` will ask for confirmation unless `--yes` is provided on the command line

### Changed

* `latch register --remote` is now the default. Use `--no-remote` to build the workflow image locally

### Fixed

* `latch register --remote` will no longer ask for host key fingerprint verification

## 2.19.6 - 2023-05-08

### Fixed

* Registry APIs should properly resolve files when using workspaces instead of always using the signer account

## 2.19.5 - 2023-05-06

### Fixed

* `latch workspace` options should be ordered alphabetically

## 2.19.4 - 2023-05-04

### Fixed

* Conda template registration issue and run in correct environment issue
# Latch SDK Changelog

## 2.19.3 - 2023-04-26

### Fixed

* Tasks stuck initializing on nodes that ran multiple tasks before

## 2.19.2 - 2023-04-24

### Dependencies

* Upgrades lytekit to `0.14.10`

## 2.19.1 - 2023-04-21

### Changed

* `latch cp` now has a progress bar for downloads (previously they only showed for uploads)

## 2.19.0 - 2023-04-14

### Added

* Functions for creating/deleting Projects and Tables, and creating Table columns in the Registry API

## 2.18.3 - 2023-04-12

### Fixed

* Registry table updates should work with columns that have spaces in names

## 2.18.2 - 2023-04-11

### Changed

* Improved Registry API `__str__` and `__repr__`

## 2.18.1 - 2023-04-08

### Fixed

* Registry API should work in an running asyncio event loop

## 2.18.0 - 2023-04-08

### Added

* A new API for interacting with Latch Registry

### Dependencies

* Added `gql` and `aiohttp` as dependencies

## 2.17.2 - 2023-04-07

### Fixed

* Prevent `latch cp` from hitting the s3 part limits which causes large
  file uploads to fail

## 2.17.1 - 2023-04-05

### Fixed

* Switched a dictionary union from `|=` notation to a manual for-loop to
  maintain support for Python 3.8.

## 2.17.0 - 2023-04-01

### Added

* Option to disable default bulk execution mechanism when an alternative (e.g. using a samplesheet parameter) is supported by the workflow itself

## 2.16.0 - 2023-04-01

### Added

* Verified Trim Galore adapter trimming workflow stub

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
  + `LatchOutputDir` used to indicate output location on Latch
  + Regex rules used to validate files
  + Splits tasks into files
  + Include empty template
  + Remove yaml metadata from docstring
  + Use messages in examples
  + Error handling
  + Add LICENSE file
  + Add README file
* Allow user to select template in GUI or pass flag
* Allow user to run `latch init .`

### Fixed

* LatchDir type transformer bug with Annotated types
  + LatchOutputDir is fixed

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

* The commands
  + `latch rm`,
  + `latch mkdir`, and
  + `latch touch`.
* The operators
  + `left_join`,
  + `right_join`,
  + `inner_join`,
  + `outer_join`,
  + `group_tuple`,
  + `latch_filter`, and
  + `combine`.

### Removed

* Removed a broken SDK test (launching CRISPResso2)

### Fixed

* `requests` library given higher version lower bound to fix warning with one of its dependencies
* `lytekit` version updated to
  + pin `numpy` to version `1.22` (breaking changes in later versions of this library)
  + have better behavior when downloading directories during local development
  + force retry on connection closed on file uploads (POST requests more generally)
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
  subclasses (eg. `torch` )
