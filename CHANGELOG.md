# Changelog

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [1.180.0] - 2025-12-22

### Added

- Added support of `scope` parameter for WebAPI access token retrieval

### Changed

### Fixed

- Fix `otp.Source.write` method writing ticks multiple times with `out_of_range_tick_action=exception` and
  multiple `db_locations`
- Do not show warnings from OneTick in Jupyter notebooks
- Fixed unstable test using `otp.math.now`
- Fixed conversions between `int` types with `astype`
- Skip failing Jupyter notebook

### Removed

## [1.179.0] - 2025-12-15

### Added

- Add `build-docs(latest)` Gitlab job
- Added parameter `tick_offset` for `otp.Source.limit`

### Changed

### Fixed

- Fixed compatibility check for `show_all_ticks` parameter
- Fixed getting onetick version from remote databases
- Reverted using native `prepend_db_name` in `otp.Symbols`
- Fixed setting end time for `OQD` sources
- Fixed symbol date passed to queries
- Fixed some tests on latest OneTick build `20251010-2`

### Removed

## [1.178.0] - 2025-12-09

### Added

- Added console command `onetick-render` for `otp.utils.render_otq`
- Added `otp.ReadFromDataFrame` source
- Added link to Github issues to the PyPI page

### Changed

- Improve time to get schema in `otp.DataSource`
- Add cache when getting schema, configured time ranges and access info of the database
- Deprecated generating ticks in `otp.Ticks` from pandas dataframe
- Improved documentation about testing

### Fixed

- Fixed `webapi` tests on latest `urllib3` version

### Removed

## [1.177.0] - 2025-12-01

### Added

- Added support of parameter `max_spread` for `ObSnapshot`, `ObSnapshotWide`, `ObSummary` and `ObSize`
- Added `unit(durations)` Gitlab test job

### Changed

- Regenerated `.test_durations` file

### Fixed

- Change addresses of the cloud servers in tests
- Fixed pytest marker for `test_otq.py`

### Removed

## [1.176.0] - 2025-11-24

### Added

- Added support of `show_all_ticks` parameter for `otp.Source.diff`
- Added support of native `keep_db` parameter implementation for `otp.Symbols`
- Add warning when passing float values and columns to `join_with_query`
- Add support of OneTick parameters to `otp.decimal` class

### Changed

### Fixed

- Fixed support of `schema_policy=manual` for ObSnapshot sources
- Fixed tick type in order book tests
- Fix `.float.str` method not working with `otp.nan` and `otp.inf` values

### Removed

## [1.175.0] - 2025-11-17

### Added

### Changed

### Fixed

- Fixed unstable test

### Removed

## [1.174.0] - 2025-11-11

### Added

- Added basic debug option for `otp.utils.render_otq` and `otp.Source.render_otq`
- Added support for creating `otp.decimal` values from strings

### Changed

- Changed default output format for `otp.utils.render_otq` and `otp.Source.render_otq` from `png` to `svg`
- Improved `otp.Source.pnl_realized` documentation

### Fixed

- Fixed copying compatibility tests results to documentation server
- Fixed `pandas` warning on python 3.12
- Improved query rendering by `otp.utils.render_otq` and `otp.Source.render_otq`
- Reduced graphs sizes and improved readability in query rendering by `otp.utils.render_otq` and `otp.Source.render_otq`
- Fixed converting floats with science notation
- Drop `OMDSEQ` column in `otp.merge(enforce_order=True)`
- Fixed size types in order book aggregations when `size_max_fractional_digits` is greater than zero

### Removed

## [1.173.0] - 2025-10-28

### Added

- Added *preliminary* support for python 3.14
- Added `locator_parser` and `onetick-lib` to unit tests
- Added `webapi-python3.14` testing job

### Changed

- Drop support for all dependent packages having python version less than 3.9
- Use python 3.12 when testing onetick-py and WebAPI
- Remove `pytz` usage from the codebase, use standard `zoneinfo` instead
- Drop support for `pandas` version less than 1.5.2
- Remove `pyarrow` dependency
- Change ownership of `/onetick-py` directory and use it in Gitlab CI/CD

### Fixed

- Fixed error when getting `help(otp.DataSource)`
- Fixed tests and compatibility checks for some OneTick builds and releases

### Removed

## [1.172.0] - 2025-10-20

### Added

- Added support for branch retrieval in `MultiOutputSource`.
- Added `otp.math.round`

### Changed

- Changed return type for `otp.math.floor`, `otp.math.ceil` and `otp.Operation.__round__` to float
- Renamed parameters `start` and `end` to `start_date` and `end_date` in `otp.Source.write`; `end_date` is now inclusive

### Fixed

- Fixed an issue where `otp.Source.write` wrote temporary columns in multi-day cases
- Improve `.float` and `.dt` accessors operation representations
- Fixed rounding operations for NaN and Infinity values
- Improve documentation and guides

### Removed

## [1.171.0] - 2025-10-13

### Added

- Added `otp.math.ceil`, `otp.math.div`, `otp.math.frand`, `otp.math.gcd`
- Added `otp.get_onetick_version`, `otp.get_username`
- Added parameter `as_table` for `otp.databases` and `otp.derived_databases`
- Added `otp.run_async`

### Changed

- Test `latest` OneTick build in `webapi-compatibility-latest` job
- Changed `otp.math.max` and `otp.math.min` implementations

### Fixed

- Do not upload `public` directory to GitHub
- Fixed using `trusted_certificates_file` parameter on latest WebAPI versions
- Fixed objects returned by `otp.math` functions

### Removed

## [1.170.0] - 2025-10-06

### Added

- Added support of `otp.eval` as `db` parameter in `otp.Symbols`
- Added support of `otp.Source` objects as tick sequence initializers
- Added support of multiple bound symbols to OB Sources

### Changed

- Use `otp.Source.where` for filtering examples

### Fixed

- Fixed script `github_release.sh` not working without configured author

### Removed

## [1.169.0] - 2025-09-29

### Added

- Added favicon to the documentation
- Added support of retrieving database descriptions in `otp.databases`
- Added `otp.config.default_username`

### Changed

### Fixed

- Fixed script `github_release.sh` not working without configured author

### Removed

## [1.168.0] - 2025-09-22

### Added

### Changed

- Changed `symbol_time` parameter of `join_with_query` to use query parameter instead of symbol parameter
- Improve parsing the value of `OTP_WEBAPI` environment variable

### Fixed

- Fixed script `github_release.sh` not working on detached HEAD

### Removed

## [1.167.0] - 2025-09-15

### Added

- Add script `github_release.sh` and a job for automatic upload to Github on release

### Changed

- Remove deprecated authentication method from WebAPI guide in documentation

### Fixed

- Ray examples in documentation updated for compatibility with latest `onetick.py` version

### Removed

## [1.166.0] - 2025-09-08

### Added

- Added `webapi` tests to release pipeline
- Added parameter `include_market_order_ticks` to `ObSnapshot*` aggregations
- Added parameter `readable_only` to `otp.databases`

### Changed

- Updated `onetick.query-webapi` to `20250727.0.1` in testing
- In `otp.databases` return only readable databases by default

### Fixed

- Fixed exception is `otp.Session` constructor corrupting future constructor calls
- Fixed truncating parameter values when using `otp.utils.render_otq`
- Fixed setting `otp.get_symbology_mapping` parameters with columns

### Removed

## [1.165.0] - 2025-09-01

### Added

- Added `otp.Source.render_otq` method

### Changed

- Changed minimum supported python version to python 3.9

### Fixed

- Updated `onetick-view` documentation reference

### Removed

## [1.164.0] - 2025-08-25

### Added

- Added `otp.Source.pnl_realized` to the use-cases documentation
- Added `.pre-commit-config.yaml`
- Added `otp.Source.write_text()`
- Added `otp.Source.insert_at_end`

### Changed

- Update `pylint` and `pylintrc` configuration file

### Fixed

- Fixed `otp.Source.dump()` method not working correctly with aggregations
- Fixed `SonarQube` errors in `locator_parser`
- Fixed error connecting Docker daemon
- Fixed logic in `otp.Source.write` with `start` and `end` parameters
- Fixed CI/CD for `pre-commit` linters

### Removed

## [1.163.0] - 2025-08-04

### Added

- Added more details and examples in `otp.join_by_time` documentation
- Added more docs and examples for `running` and `all_fields` aggregation parameters

### Changed

- Make the button in the documentation not copy the console output and prompts

### Fixed

- Fixed `deploy` job in gitlab pipeline
- Fixed `send_onetick_release.sh` script
- Fixed `wheel-build-test` job in gitlab pipeline for patch releases

### Removed

## [1.162.0] - 2025-07-30

### Added

- Make `onetick-py` public, upload to PyPI
- Move `locator_parser` package to `onetick-py` source code, remove dependency
- Move `onetick-lib` package to `onetick-py` source code, remove dependency

### Changed

- Migrate from `setup.py` to `pyproject.toml` package build
- Change `README.md` and installation sections in the documentation
- Removed `NYSE_TAQ` mentions from the code
- Use `_SAMPLE` databases in the examples and the documentation

### Fixed

### Removed

- Remove output from Jupyter notebooks files (it is generated dynamically)
- Remove `setup.cfg`

## [1.161.0] - 2025-07-28

### Added

- Added parameter `symbol` to `_inspection.DB.ref_data()`
- Added `otp.aggregations.linear_regression` and `otp.Source.linear_regression`

### Changed

- Set parameter `prefer_speed_over_accuracy` in `DbShowLoadedTimeRanges` by default

### Fixed

- Fixed `otp.decimal` object operators' methods
- Fixed `otp.decimal` scientific notation

### Removed

## [1.160.0] - 2025-07-14

### Added

- Added `otp.get_symbology_mapping()`

### Changed

- Changed `_inspection.DB.access_info` property to function and added parameter `deep_scan` and `username`

### Fixed

- Changed compatibility option to disable stripping quotes around JWQ parameters in latest onetick build
- Fixed converting fields to OneTick integer types

### Removed

## [1.159.0] - 2025-07-07

### Added

- Added parameter `groups_to_display` to all aggregations
- Added `otp.Operation.str.ilike`

### Changed

### Fixed

### Removed

## [1.158.0] - 2025-06-30

### Added

- Added `otp.types.datetime2timeval()`

### Changed

- Refactoring of the functions setting start and end time of the query
- Changed some deprecated database names in documentation tests

### Fixed

- Disabled use of `concurrency` parameter in `otp.run` with WebAPI for older versions of OneTick
- Fixed saving start and end time expressions in .otq file
- Fixed using `otp.datetime` objects as default start and end time

### Removed

## [1.157.0] - 2025-06-23

### Added

- Added parameter `offset` to `otp.Ticks`
- Added support for removing offsets in `otp.Ticks` when `offset` set to None
- Added support of generic aggregations for `otp.Source.agg` / `otp.agg.compute`

### Changed

- Improved performance of getting first ticks in `otp.Source.__getitem__`

### Fixed

- Fixed creating `otp.Source` from `pandas.DataFrame` on some `pandas` versions

### Removed

## [1.156.0] - 2025-06-16

### Added

- Add test for `.str` accessor in per-tick script
- Support setting `symbol_date` in `otp.Source.point_in_time`
- Support setting `source` as the path to the query in `otp.Source.point_in_time`

### Changed

### Fixed

- Fixed some errors, warnings and dependencies on python 3.12
- Fixed case when using tick set `.find` method on different integer types
- Markdown documentation now properly set headers level for `### Examples` and `#### SEE ALSO` sections
- Allow run queries with `otp.SqlQuery` without setting default start/end time

### Removed

## [1.155.0] - 2025-06-02

### Added

### Changed

- Moved `locator_parser` to 1.0.8, `onetick-py-test` to 1.2.7, `onetick-lib` to 1.0.11
  and updated `onetick-init` submodule to pick up LICENSE files

### Fixed

### Removed

## [1.154.0] - 2025-05-26

### Added

### Changed

- Renamed `database` parameter to `db` in `otp.FindSnapshotSymbols`

### Fixed

- Fix support of `otp.Source.join_with_snapshot` for older version of OneTick
- Fix `symbol_name_in_snapshot` parameter processing in `otp.Source.join_with_snapshot`

### Removed

## [1.153.0] - 2025-05-19

### Added

### Changed

### Fixed

### Removed

## [1.152.0] - 2025-05-13

### Added

- Added `LICENSE` file
- Added snapshots related sources: `otp.ReadSnapshot`, `otp.ReadSnapshot`, `otp.ReadSnapshot`
- Added snapshots related `otp.Source` methods: `otp.Source.save_snapshot`, `otp.Source.join_with_snapshot`

### Changed

- Changed default compression type to `NATIVE_PLUS_ZSTD` for generated databases

### Fixed

### Removed

## [1.151.0] - 2025-04-28

### Added

- Added trigonometric function: `otp.math.cot`
- Added reverse trigonometric functions: `otp.math.asin`, `otp.math.acos`, `otp.math.atan` and `otp.math.acot`
- Added docs and examples for aggregating over `Operation`

### Changed

- Convert keys of `db_properties` passed to `otp.DB` constructor to be lowercase

### Fixed

- Vector based search in documentation now turns off, when corresponding API is not available
- Fixed strict requirements for python versions again

### Removed

## [1.150.0] - 2025-04-21

### Added

- Added tests for OneTick database views

### Changed

### Fixed

- Fixed `strict` requirements for python versions other than 3.9
- Optimize expression `in range(...)` in per-tick script
- Fixed setting timestamps in `otp.Tick` and `otp.Ticks` when query time interval is small

### Removed

## [1.149.0] - 2025-04-15

### Added

- Added more docs about `bucket_interval` parameter
- Added support for `otp.Milli` bucket interval

### Changed

- Change examples getting trades to use `.character_present()` method
- Improved warning and exception message when setting schema in `otp.DataSource`

### Fixed

- Fixed passing bound symbols when `symbol_date` parameter is specified
- Support using `otp.Source.point_in_time` and `otp.PointInTime` without `otp.config.default_symbol` set
- Fixed passing complex types in schema in `otp.DataSource`
- Fixed Windows tests after `numpy` requirements update

### Removed

## [1.148.0] - 2025-04-08

### Added

- Added `otp.Source.corp_actions` (alias to `otp.functions.corp_actions`)
- Added more examples with `otp.run` and `otp.Source.join_with_query` about query end time
- Added more examples about `otp.string` type and its limitations
- Added notification when gitlab pipeline failed on master branch too

### Changed

- Improved performance metrics gathering documentation
- Updated `numpy` package dependency up to versions 2.2.X

### Fixed

- Fix documentation for `otp.Source.add_prefix` and `otp.Source.add_suffix`
- Fixed `start` and `end` parameters logic in `otp.Source.join_with_query` if `datetime` objects are used
- Fixed image name in `notify-release-pipeline-failed` job

### Removed

## [1.147.0] - 2025-03-31

### Added

- Added return type to the `otp.Source.agg` documentation
- Added `otp.config.disable_compatibility_checks`
- Add parameter `symbol_date` for `otp.merge`, `otp.DataSource` and many internal functions
- Add `otp.meta_fields.symbol_time`

### Changed

### Fixed

- Support using `otp.Source.process_by_group` without `otp.config.default_symbol` set

### Removed

- Removed `otp.Source._to_otq_tmp_file()` function

## [1.146.0] - 2025-03-24

### Added

- Added support of tick and multi-column output aggregations in `otp.Source.agg`

### Changed

- Send notification if release pipeline failed

### Fixed

- `otq.config.API_CONFIG` must be `otq.API_CONFIG` to resolve conflict with latest `onetick.query_webapi`

### Removed

## [1.145.0] - 2025-03-17

### Added

### Changed

- `schema_policy` forced to be 'manual' if schema is set via `otp.DataSource` constructor

### Fixed

- Remove `ticks` bucket units from Order Book aggregations

### Removed

## [1.144.0] - 2025-03-10

### Added

### Changed

### Fixed

- Improve `.str` accessor operation representations
- Fixed setting locator date range in `_inspection.DB` when date exceeds python maximum value
- Aggregation functions with arguments now will pass pylint checks correctly

### Removed

## [1.143.0] - 2025-03-03

### Added

- Added `otp.agg.implied_vol` and `otp.Source.implied_vol`

### Changed

- Documentation now utilize vector based search for better results on natural language queries.

### Fixed

- `OTP_SKIP_OTQ_VALIDATION` bug fixed

### Removed

## [1.142.0] - 2025-02-24

### Added

### Changed

### Fixed

- Fixed compatibility check and tests for `otp.Source.write_parquet`

### Removed

## [1.141.0] - 2025-02-17

### Added

- Added support of performance metrics gathering for `otp.Session`
- Added documentation with examples about changing contexts
- Added `otp.ObSize`, `otp.agg.ob_size` and `otp.Source.ob_size`
- Added `otp.ObVwap`, `otp.agg.ob_vwap` and `otp.Source.ob_vwap`
- Added `otp.ObNumLevels`, `otp.agg.ob_num_levels` and `otp.Source.ob_num_levels`

### Changed

### Fixed

### Removed

## [1.140.0] - 2025-02-10

### Added

- Added support of passing bound symbols to `otp.join_by_time`

### Changed

- OneTick server for WebAPI tests upgraded to 20241220-1
- Cleaner markdown documentation build

### Fixed

- `OTP_SKIP_OTQ_VALIDATION` now respected again
- Fixed `otp.compatibility.is_supported_point_in_time` check

### Removed

## [1.139.0] - 2025-02-03

### Added

- Added parameter `columns` to `otp.Source.fillna` function
- Support passing list to `otp.Operation.isin`
- Added `otp.Source.virtual_ob`
- Support datetime offset objects as parameter `bucket_interval` in `otp.Tick`

### Changed

- Improve documentation of `.str.contains()` function

### Fixed

### Removed

## [1.138.0] - 2025-01-27

### Added

- Added optional WebAPI compatibility test for 1.25 release
- Added `otp.timedelta` and support it to be used as time offset in `otp.Tick` and `otp.Ticks`
- Support changing input field names in `otp.Source.pnl_realized`
- Added docs for `otp.Source.add_fields`

### Changed

### Fixed

- Fixed support of setting `skip_tick_if` to `otp.nan` in `otp.agg.first` and `otp.agg.last`
- Documentation publishing now removes stale HTML files.

### Removed

## [1.137.0] - 2025-01-20

### Added

- Added `otp.Source.limit`
- Added missing docs for `time_series_type` parameter in supported aggregations
- Support getting onetick version from different server and context
- Added parameter `check_index_file` for some functions in `_inspection.DB` class
- Added more details and links in the docs of datetime parameters

### Changed

- Improve docs for `use_rename_ep` parameter in `otp.join_by_time`

### Fixed

- Fixed behaviour when regular expressions are used in `otp.Source.drop`

### Removed

## [1.136.0] - 2025-01-13

### Added

- Added more documentation for `otp.join_by_time`
- Added `otp.Source.return_ep` and `otp.aggregations.return_ep`
- Added `otp.Source.multi_portfolio_price` and `otp.aggregations.multi_portfolio_price`
- Added value `local_number_of_cores` to `otp.config.default_concurrency`
- Added `otp.config.presort_force_default_concurrency`

### Changed

### Fixed

- Fixed some more SonarQube issues
- Compatibility with `FIND_VALUE_FOR_PERCENTILE` parameters on the latest build `20241220`
- Fixed docs for `otp.oqd.OHLCV`

### Removed

- Cell outputs removed from markdown documentation build

## [1.135.0] - 2025-01-06

### Added

### Changed

- Replaced some `otq.run` calls with `otp.run`
- Improve and refactor OQD docs
- Migrated to new pip server (DO-1840)

### Fixed

- Fixed `send_onetick_release.sh` script after FTP server update again

### Removed

## [1.134.0] - 2024-12-23

### Added

- Added automatic access token obtaining for WebAPI via `access_token_url` parameter

### Changed

- Move some WebAPI-related files to `./webapi` directory and ignore it in SonarQube

### Fixed

- Fixed one more typo in the installation docs
- Fixed parsing OneTick version after format change

### Removed

- Remove `otp.laser` hardcoded server address

## [1.133.0] - 2024-12-16

### Added

- Added `otp.Source.character_present`
- Added `otp.Source.estimate_ts_delay()`

### Changed

- bestex cases become part of public documentation (getting started)
- Require correct version of `polars` on python3.8

### Fixed

- Fixed typo in the installation docs
- Fixed deprecation warnings in docs
- Fixed some `SonarQube` issues
- Fixed `otp.Source.write` method to write data without specifying symbol name
- Fixed `send_onetick_release.sh` script after FTP server update

### Removed

## [1.132.0] - 2024-12-09

### Added

- Added `otp.Source.portfolio_price` and `otp.agg.portfolio_price`

### Changed

- Clarified when onetick-py with local binaries is useful in the Installation docs.
- Use `otp.config.default_db` when getting OneTick version for compatibility checks

### Fixed

- OQD sources and EPs now compatible with WebAPI mode.

### Removed

## [1.131.0] - 2024-12-02

### Added

- Added `otp.Source.book_diff()`
- Added `otp.Source.mkt_activity()`
- Added support for `polars` output structure in `otp.run` in WebAPI mode

### Changed

- Deprecate `tick_type` and `find_params` parameters in `otp.Symbols`
- Changed compatibility requirement for `point_in_time` method and source

### Fixed

- Fixed setting fixed-length string fields in tick objects

### Removed

## [1.130.0] - 2024-11-25

### Added

- Added `otp.Source.mkt_activity()`
- Added `_inspection.DB.ref_data` method
- Added support for `process_ticks` callback method for WebAPI

### Changed

- Adding SonarQube exclusion for the tests folders
- Ignore warning from `polars` that caused exception in WebAPI tests

### Fixed

- Fixed `_inspection.DB.symbols()` truncating symbol names
- Make `TestAcceleratorDbWrite` test even more stable

### Removed

## [1.129.0] - 2024-11-18

### Added

### Changed

- Updated SonarQube configuration to use gitlab template and get code coverage

### Fixed

### Removed

## [1.128.0] - 2024-11-12

### Added

- Added data quality and symbol errors methods
- Add `_inspection.DB.show_archive_stats` method
- Added support of setting `trusted_certificates_file` in WebAPI `run`
- Added `otp.Source.fillna`

### Changed

- Added explicit `schema` named parameter instead of `**kwargs` in the `otp.Source`
- Upgraded used WebAPI package to `20241018.0.0`
- Update documentation about `bucket_units` parameter in aggregations
- Improve documentation and add examples for `otp.Symbols`

### Fixed

- Make `TestAcceleratorDbWrite` test more stable
- Fixes for documentation of `count`, `head` and `tail` functions
- Fixed segfault in derived databases test on older OneTick builds
- Fixed test on python 3.12
- Fixed `adjustment_date` parameter validation for `otp.corp_actions`
- Fixed OneTickLib singleton initialization when getting OneTick version

### Removed

- Removed unused `otp.types.datetime_now` class

## [1.127.0] - 2024-10-28

### Added

- Added `otp.Source.point_in_time` and `otp.PointInTime`
- Added more docs for `schema_policy` parameter and examples in `otp.DataSource`
- Added support for `otp.Operation` objects as parameter in `fillna` method
- Added `otp.agg.find_value_for_percentile` and `otp.Source.find_value_for_percentile`
- Added documentation for Onetick parallelization and tick processing

### Changed

- Refactored `otp.sources` into separate files
- Raise exception if schema is not specified when `manual_strict` policy is used in `otp.DataSource`
- Added warning about using `otp.Operation` (and inherited classes like `otp.Column`) in format expressions

### Fixed

- Fixed examples and indentation in docstrings in aggregation methods in `otp.Source`
- Fixed bug with uncleared list in `otp.CSV`
- Fixed some more `SonarQube` issues

### Removed

## [1.126.0] - 2024-10-21

### Added

- Added `otp.Source.pnl_realized` method
- Added `otp.DataFile` source class
- Added `utils.query_properties_to_dict`, `utils.query_properties_from_dict` and `utils.symbol_date_to_str` functions
- Support string as `symbol_date` parameter in `otp.run`

### Changed

- Strict dependencies for MacOS to avoid issues with `pyarrow`
- Changed and added more docs for `otp.DB`
- Raise warning when when some parameters are specified without `src` in `otp.DB`
- Renamed `otp.Source._save_as_tmp_otq` to `otp.Source._to_otq_tmp_file`
- Refactor `otp.Source._get_date_range`
  into `otp.Source.__get_common_symbol` and `otp.Source.__get_modify_query_times` functions
- Renamed `otp.Source._get_date_range` to `otp.Source._set_date_range_and_symbols` function

### Fixed

- Fixed authentication tests with default pytest warnings
- Fixed parameter name in `write_parquet` on latest OneTick builds

### Removed

## [1.125.0] - 2024-10-09

### Added

- Added `otp.Source.lee_and_ready`
- Added parameter `file_contents` to `otp.CSV`
- Added `otp.agg.standardized_moment` aggregation

### Changed

- Exclude `conftest.py` files from `src` directory when building distribution
- Move `oqd.sources` doctests and mark them as `integration` tests
- Use latest WebAPI in packages compatibility tests

### Fixed

- Fixed support of using symbol params object in `otp.Source` symbol parameter
- Fixed default value of `adjustment_date` in `otp.corp_actions`
- Fixed logic to process `otq.run` result on the newest OneTick versions
- Fixed failed test with changed error message for `EXPECT_LARGE_INTS=true` on latest OneTick build 20240812120000

### Removed

## [1.124.0] - 2024-09-30

### Added

- Added `otp.Source.diff`
- Added `otp.agg.exp_w_average` and `otp.agg.exp_tw_average` aggregations

### Changed

- Updated SonarQube Configuration

### Fixed

### Removed

## [1.123.0] - 2024-09-23

### Added

- Documentation page for WebAPI configuration of on-prem OneTick server.
- Added support of parameters `SKIP_TICK_IF`/`FWD_FILL_IF` as `skip_tick_if` in `otp.agg.first` and `otp.agg.last`
- Added support of parameter `null_int_val` for `otp.agg.first`, `otp.agg.last`, `otp.agg.min` and `otp.agg.max`
- Added `otp.db._inspection.DB.show_config()`

### Changed

- Don't raise exception if we can't detect database schema automatically in `otp.DataSource`
- WebAPI mode is automatically selected, when no `onetick.query` found, but `onetick.query_webapi` is installed.

### Fixed

- Fixed `setup.py` not working correctly in Gitlab runner

### Removed

## [1.122.0] - 2024-09-16

### Added

- Added `otp.SqlQuery`
- Added `otp.agg.percentile` aggregation
- Added more docs about default schema policy in `otp.DataSource`

### Changed

- Change project structure, move source code to `./src` and tests to `./tests`
- Re-generated `.test_durations` file
- Add presort in all cases if parameter `presort=True` in `otp.merge`
- Upgraded `onetick.query_webapi` to the latest version `20240812.0.2`

### Fixed

- Allow using symbol parameters in `.apply()` and per-tick script functions
- Fixed type annotation for python3.8
- Fix `build.sh` script and `sonar-project.properties` after changing project structure
- Fixed `otp.derived_databases` when database with acl restrictions is used

### Removed

## [1.121.0] - 2024-09-09

### Added

- Added support of using symbol and query params in `onetick.py.sources.update_node_tick_type`
- Added support of using symbol and query params as `symbol` parameter of `otp.Source`
- Added support of `WHEN_TICKS_EXIT_WINDOW` as value for `all_fields` in aggregations
- Added `otp.Source.skip_bad_tick` method

### Changed

- Refactor `otp.Source` into separate files

### Fixed

### Removed

## [1.120.0] - 2024-09-02

### Added

- Added parameter `timestamp_format` for `otp.CSV`
- Added docs for `otp.__version__`, `otp.__build__` and `otp.__main_one_tick_dir__`

### Changed

- Changed `otp.config.default_concurrency` default value to 0 or 1
- Changed `concurrency` parameter in `otp.merge` and `otp.DataSource` to inherit default value from the original query

### Fixed

- Fixed script `send_onetick_release.sh` after gitlab pipeline stage was renamed
- Fixed `otp.TmpFile` not closing file descriptor

### Removed

## [1.119.0] - 2024-08-26

### Added

### Changed

### Fixed

- Fixed some SonarQube issues
- `otp.run()` arguments `start` or `end` now respects the timezone both for timezone-aware `otp.dt` and `datetime.datetime`.
- Fixed managing temporary directories and logging files removal when using pytest
- Fixed compatibility check with unstable test with cache
- Fixed version switcher in documentation
- Fixed multiversion documentation deployment for patch versions, cosmetics update on version switcher UI

### Removed

## [1.118.0] - 2024-08-19

### Added

- Added `otp.config.default_schema_policy`
- Added docs mentioning `onetick.hosted` project
- Support for loop with range in per-tick script
- Added `otp.db._inspection.DB.access_info()`
- Added parameter `apply_rights` to `otp.corp_actions`
- Support setting `bucket_interval` as a symbol parameter
- Added article `How to use onetick.query with onetick.py`

### Changed

- Updated docs for `schema_policy` parameter
- Default context in WebAPI mode is now `None`

### Fixed

- Compatibility check bug when no `DB_LOCATOR.DEFAULT` set in `one_tick_config.txt` file.
- Database inspection methods now check database ACL restrictions
- Visual improvements on AI generated search results
- Fixed compatibility issues for some tests
- Fixed doc for `otp.agg.max`

### Removed

## [1.117.0] - 2024-08-12

### Added

- Separate installation page for remote Cloud Server usage through WebAPI.
- Added `webapi` extra to setup.py for installing `onetick.query_webapi` module with simple command `pip install onetick-py[webapi]`

### Changed

### Fixed

### Removed

## [1.116.0] - 2024-08-05

### Added

- Added support of `otp.param` in `dest_symbology` parameter of `otp.SymbologyMapping`
- Added links to `onetick view` documentation in the docs

### Changed

- Disable deploy jobs in OT `pre_candidate` tests pipelines
- Changed logging format for `otp.DataSource` and `otp.run` parameters to json

### Fixed

### Removed

## [1.115.0] - 2024-07-29

### Added

- Added WebAPI support through `onetick.query_webapi` module
- Added documentation about data inspection and `otp.DB`

### Changed

- Changed default value for parameter `schema_policy` in `otp.DataSource` docs
- Added parameter `inplace`, removed parameter `move_node` and changed returned value in `otp.Source.sink`
- onetick-ml now included to product tests

### Fixed

### Removed

## [1.114.0] - 2024-07-22

### Added

- Added parameter `enforce_order` for `otp.merge`
- Support `otq.GraphQuery` as parameter `symbols` in `otp.DataSource` and `otp.merge`

### Changed

### Fixed

- Fixed error message in per-tick script inner functions
- Small fixes for `Order Book Analytics` and `Symbologies` getting started guides

### Removed

## [1.113.0] - 2024-07-15

### Added

### Changed

- Updated `pandas` package dependency up to versions 2.2.X
- Updated `twine==5.1.1`
- Simplify graph when using query parameters in `otp.Ticks`
- Refactored and unified default value for tick type parameter for sources
- Uncomment unstable test with cache
- Don't use deprecated `StrictVersion` from `distutils`

### Fixed

- Fixed support for big integers in `otp.Tick` and `otp.Ticks`
- Fixed inconsistency when using `Time` alias for `TIMESTAMP` field
- Fixed returning list of symbols with colons from `otp.db._inspection.DB.symbols()`

### Removed

## [1.112.0] - 2024-07-08

### Added

- Added support of reading and writing Parquet databases via `otp.ReadParquet` and `otp.Source.write_parquet`
- Added docs for `otp.SymbologyMapping` and `otp.SplitQueryOutputBySymbol`

### Changed

- Use virtual environment in Windows test job

### Fixed

### Removed

## [1.111.0] - 2024-06-24

### Added

- added post-processing for markdown docs
- Added function `otp.utils.render_otq` for rendering otq-files
- Added `otp.config.clean_up_tmp_files`
- Added logging for classes in `otp.utils.temp`
- Added more docs and tests for `.str.token()` function

### Changed

### Fixed

- Fixed `otp.Source.render` method
- Fixed in `otp.compatibility` parsing some release string format
- Fixed `otp.Session` removing files even if `clean_up` is set to `False`
- Fixed destructor for `otp.TmpFile`
- Support infinite loops in per-tick script

### Removed

## [1.110.0] - 2024-06-17

### Added

- added compatibility check for support of quotes in query parameters
- Added parameter `data` to `otp.Tick`
- Added RAG answering in documentation search results

### Changed

- Disable deploy jobs in frozen tests pipelines
- `otp.config.default_concurrency` could be set `None`

### Fixed

- Fix script `send_onetick_release.sh` not working on old minor release versions
- Fixed setting variables when initializing onetick-py with `OTP_SKIP_OTQ_VALIDATION`
- Fix problem in `otp.Ticks` when using field names equal to parameter names of `otp.Tick`
- `otp.CSV()` now handles `bool` columns (with `true`/`false` values only)

### Removed

## [1.109.0] - 2024-06-10

### Added

- Added build and test jobs for OT candidate build pipeline

### Changed

- Update `onetick-init==0.0.3`

### Fixed

- Compatibility check now also checks the release patch date
- Fixed error when passing `pandas` offset objects to `otp.Ticks`

### Removed

## [1.108.0] - 2024-06-03

### Added

- Added `otp.bit_xor`, `otp.bit_not` and `otp.bit_at`

### Changed

- Improve example with `otp.DB` creation in docstring
- Use `FutureWarning` instead of `DeprecationWarning` in user-facing interfaces

### Fixed

- Fixed `test_remote`
- Make `otp.agg.generic` doctest faster
- Forbid adding or updating meta fields

### Removed

## [1.107.0] - 2024-05-27

### Added

### Changed

- Revert saving tick sequence initializer sub-queries to the same file

### Fixed

- Removed error when saving `otp.eval` sub-query twice
- Fixed error reported by `sonar`
- Fixed `otp.merge` performance with merging "diamond" pattern

### Removed

## [1.106.0] - 2024-05-20

### Added

- Added parameter `generate_separate_file_only` for `otp.eval`

### Changed

### Fixed

- Allow empty dataframe in `otp.Ticks`

### Removed

## [1.105.0] - 2024-05-13

### Added

- Added class `otp.param` for OneTick query parameters
- Added support for `otp.param` in `bucket_interval` parameter in aggregations
- Added `otp.perf`
- Added parameters `running`, `start_time_expression` and `end_time_expression` to `otp.Source.to_otq`

### Changed

- Refactor `otp.Operation` and `otp.Column`
- Save `eval` and `create_cache` sub-queries in main generated .otq file

### Fixed

- Fixed recursion errors in complex `otp.Operation` objects
- Fixed docs for `otp.join_by_time`
- Fixed test with escaped quotes on latest build
- Fixed tick server termination in `test_cep.py`

### Removed

- Removed `otp.Source.use_name_for_column_prefix()`
- Removed `_ParamColumn` class

## [1.104.0] - 2024-05-06

### Added

### Changed

- Changed default value for parameter `how` in `otp.join` to `left_outer`, `outer` value is deprecated
- In `otp.DataSource` use `Passthrough` instead of `Merge` when only one symbol is specified

### Fixed

- Fixed `otp.run` warning about missing query start/end time, if it's specified at least in one `otp.Source` in query
- Fixed compatibility errors on some tests

### Removed

## [1.103.0] - 2024-04-29

### Added

- Add testing all recently added types in `otp.Source.table`
- Added functions `otp.Source.cache` and `otp.modify_cache_config`

### Changed

### Fixed

- Fixed some problems pointed by `SonarQube` linter

### Removed

## [1.102.0] - 2024-04-22

### Added

- Added parameter `default_tick` for `otp.agg.first_tick` and `otp.agg.last_tick`
- Tests for `join_with_query` caching
- Parameter `process_query_async` for `join_with_query`
- Added `SonarQube` linter checks in gitlab CI
- Internal documentation added to GitLab Pages with Markdown files published as archive

### Changed

- in `otp.Source.write` function change default value of parameter `append` to `False`
- in `otp.db.write_to_db` function set default value of parameter `append` to `True`

### Fixed

- Fixed `option_price` doctests for OneTick build `20240330-1`
- Fixed some integer types couldn't be converted to string
- Fixed compatibility errors on some tests

### Removed

- Removed `otp.Source.symols_for` and `_MultiSymbolsSource`
- Removed `otp.utils.type2str`, `otp.utils.value2str`, `otp.utils.time2nsectime`

## [1.101.0] - 2024-04-15

### Added

- Added `otp.int` and `otp.long`
- Support setting query properties with dictionary
- Added support of `float` values in `otp.Milli` and `otp.Second`

### Changed

### Fixed

- Fixed `otp.agg.vwap` not supporting int and float subclasses
- Fixed cache tests
- Updated docs about start and end times and symbols parameters in `otp.DataSource`
- Fixed parsing query properties with `=` character

### Removed

## [1.100.0] - 2024-04-08

### Added

- Added support of using `dateparts` in `otp.Tick` `offset` parameter
- Added basic support of caching event processors via `otp.create_cache`, `otp.delete_cache` and `otp.ReadCache`
- Added `otp.config.otq_debug_mode` to keep all generated otq files and log their paths
- Added support of passing bucket end condition with `bucket_interval` parameter in aggregations
- BestEx / TCA use-cases into the documentation

### Changed

### Fixed

### Removed

- Removed `otp.ZERO_TIME` and `otp.INF_TIME`

## [1.99.0] - 2024-04-01

### Added

- Added ability to pass `date` into the `otp.Tick` constructor to use entire day for the `start` and `end` parameters
- Added `otp.Source.pause`
- Added parameter `source_fields_order` to `otp.join_by_time`
- Added `otp.callback.ManualDataframeCallback`
- Added parameter `manual_dataframe_callback` to `otp.run`

### Changed

- Use cases in documentation has split to separate document, one per use case

### Fixed

- pytest lookup for Windows picks up the sphinx configuration

### Removed

## [1.98.0] - 2024-03-25

### Added

- Added `otp.Source.add_fields` method
- Added `otp.config.allow_lowercase_in_saved_fields`

### Changed

- unskip some doctests on linux
- Update `onetick-lib==1.0.7`

### Fixed

- Fix bug with different types of offset in `otp.Ticks`
- support `join_with_query` parameter used as `otp.eval` parameter

### Removed

## [1.97.0] - 2024-03-18

### Added

- Added `otp.join_with_aggregated_window`

### Changed

### Fixed

### Removed

## [1.96.0] - 2024-03-11

### Added

- Added support of `num_ticks_per_timestamp` parameter to `otp.Tick`
- Added `modify_from_query` method for state variables
- Added `otp.config.logging` parameter and `otp.log` module
- Added `otp.hash_code` function
- Added description of how to generate markdown documentation using the sphinx

### Changed

- compatibility docs now represent earliest supported version of OneTick
- `onetick-query-stubs` installation command now have extras with Ray version specified
- `otp.run` will throw warning if start or end time is not set and there is no default value
- updated `locator_parser==1.0.7` (fixing problem with not closed file in `locator_parser.FileReader`)
- simplified some `locator_parser.FileReader` calls
- Improved doc for `.num_distinct` method with pointing it to corresponding EP in OneTick
- Improved doc for inspection database (result of `opt.databases()`)
- `bucket_units` in aggregations could be omitted if `bucket_end_condition` used
- Updated docs for `otp.DB`

### Fixed

- Fixed rules variables for `trigger-product` jobs
- Fix unstable `test_multi_symbol` test
- Fixed parameter `db` in `otp.DataSource` when used as a OneTick parameter
- fix passing parameter column as `pattern` param of `otp.Symbols`
- Fix hashing tests for Windows

### Removed

## [1.95.0] - 2024-02-28

### Added

- `benchmark_comparisons` section with 'option_price' values comparison to online benchmark
- Added `otp.bit_and` and `otp.bit_or` functions
- Added example with skipping doctest on Windows
- Added `is_event_processor_repr_upper` and `is_date_trunc_fixed` to `otp.compatibility`

### Changed

- `otp.state.tick_set.find` parameter `default_value` made to by default for the type of the requested field
- Skip `test_redirect_logs` on Windows

### Fixed

- Fix `otq._internal_utils.get_reference_counted_prefix` calls due to function rename
- Fixed tests failing on `20240205120000` OneTick build

### Removed

## [1.94.0] - 2024-02-20

### Added

- Test `otp.derived_databases` in empty session
- Add docs for `otp.Source.get_name` and `otp.Source.set_name`
- Added more docs and examples to `otp.query`
- Added `otp.config.main_query_generated_filename`

### Changed

- Set columns for empty dataframe returned by `otp.run` based on source schema

### Fixed

- Don't run downstream pipelines on changelog commits
- Fixed default value of parameter `time_series_type` for `otp.agg.tw_average`
- `otp.ODBC` now checks OneTick version for compatibility

### Removed

## [1.93.0] - 2024-02-12

### Added

- Added support of `otp` time units as `bucket_interval` in aggregations
- Added support of context managers for all `otp.config` parameters

### Changed

- `otp.Source[:n]` use `FIRST_TICK` aggregation instead of filtering by ticks indexes
- Test `sol` and `bestex-py` as a multi-project trigger pipeline in Gitlab CI/CD

### Fixed

- Fix `test_redirect_logs` on Windows
- fix development requirements to be compatible with Python 3.7-3.11

### Removed

## [1.92.0] - 2024-02-05

### Added

- add example with using unmapped values as default to `.map()` method docs

### Changed

### Fixed

- Fixed parsing `adjustment_date` parameter in `otp.corp_actions` when value is datetime

### Removed

## [1.91.0] - 2024-01-29

### Added

- Added `Source.if_else` function
- Added `otp.ODBC`
- `otp.DB`: added parameters `minimum_start_date` and `maximum_end_date`
- Added `otp.config.ignore_ticks_in_unentitled_time_range`

### Changed

- Changed link in the documentation logo
- Updated the Overview page in the docs
- Removed exception not allowing to add data to derived database before using it on the latest OneTick builds

### Fixed

- Inspect remote query in case it is located locally too
- Fix case when remote query is applied without known pins in `.apply` method
- `otp.query`: fixed parsing string parameters with quotes

### Removed

## [1.90.0] - 2024-01-22

### Added

- Added `otp.__main_one_tick_dir__`, `otp.__one_tick_bin_dir__` and `otp.__one_tick_python_dir__` properties
- Added support numeric type conversion for bool `otp.Operation`

### Changed

- Complain if OneTick bin directory is not specified in PATH env variable on Windows
- Improve logging in `test_redirect_logs`

### Fixed

- Fixed custom start and end times in order book sources
- Fixed error when doing `.push_back()` in empty tick list
- Fixed error when using datetime difference in per-tick script
- Make `test_redirect_logs` and `test_use_many_dbs` less unstable

### Removed

## [1.89.0] - 2024-01-15

### Added

- Added an example into the `Source.agg` API doc for the `flexible` bucket interval parameter
- Added test checking segfault when reloading locator
- Added `otp.Source.update_timestamp()` method
- parameter `large_ints` for `first`, `last`, `min`, `max` aggregations

### Changed

- refactor changelog sections
- set `autosectionlabel_maxdepth=2` when generating Sphinx documentation
- improved parsing OneTick version in `otp.compatibility`
- turned warnings into errors

### Fixed

- The `otp.corp_actions` function doesn't change passed source, and copy it instead
- fixed all warnings in tests, made some refactoring
- fixed setting output schema in aggregations without input column
- fixed converting `SymbolType.get()` default value to string
- fixed wrong schema detection in `otp.DataSource` when tick type is a parameter
- don't raise warning when using `otp.nsectime(0)`
- fixed not closed files when using `locator_parser.FileReader`
- fix adding fields from parameters in `otp.DataSource` with `manual_strict` schema policy
- Fixed trying to find field of `otp.string` type with tick set's `.find()` method

### Removed

## [1.88.0] - 2024-01-01

### Added

- add `otp.throw_exception` per-tick script function
- added `otp.Source.where_clause`
- added `otp.Source.where`
- add `otp.ObSummary`, `otp.agg.ob_summary` and `Source.ob_summary`
- added info about logging symbols in `Debugging` guide

### Fixed

- fix `otp.datetime.end` behaviour if next day is after DST/timezone transition time

### Changed

- do not skip fault tolerance test
- warning message regarding subtraction of timestamps marked as deprecated now and ask user to explicitly specify time unit

## [1.87.0] - 2023-12-25

### Added

- add `encoding` parameter to `otp.run`
- added documentation for generating ticks in "Getting started" guide
- added documentation about `copy_tick` method available on input tick object in per-tick script
- checking correctness of created field names

### Fixed

- fixed script `send_onetick_release.sh` failing in Gitlab job again
- fixed logic for parameter `redirect_logs` in `otp.Session`
- updated command to install `onetick-query-stubs` with specified `onetick-py` version

## [1.86.0] - 2023-12-19

### Added

- added `otp.logf` per-tick script function and `otp.Source.logf` method
- debugging chapter in the documentation
- add `otp.Source.modify_symbol_name`

### Fixed

- fixed building documentation
- fixed using remote queries in `otp.query`

### Changed

- update onetick-py-test version to pick up the `--show-stack-trace` feature

## [1.85.0] - 2023-12-11

### Added

- parameter `log_symbol` for `otp.run`

### Fixed

- fixed script `send_onetick_release.sh` failing in Gitlab job
- fixed output types after aggregations `apply()` method
- fixed parsing config with include and env vars
- sphinx documentation for testing

### Changed

- updated `Source.agg()` documentation to indicate that `end_condition_per_group=True` applies to all bucketing conditions

## [1.84.0] - 2023-12-04

### Added

- info about symlinks on Windows in `CONTRIBUTING.md`
- script to upload source code and docs of latest onetick-py release to Slack and FTP
- support for comments and included files in `otp.utils.get_config_param`
- improved documentation for `otp.Symbols` `pattern` parameter
- ability to pass desired schema to created tick sequences
- added missing parameters to `otp.merge`

### Fixed

- printing datetime subtraction warning
- fix unit tests after bug with `eval` truncating timestamps in tick sets was fixed in OneTick release

### Changed

- improved documentation in different places
- disallow `otp.Tick` creation with `bucket_time=end` and set non-zero `offset` params
- parameter `symbol` of `otp.Query` constructor is limited to be only `None` or `otp.adaptive`

## [1.83.0] - 2023-11-13

### Added

- added `mypy` static type checker to CI/CD pipeline
- added new string accessor function `like()`
- `otp.Source.throw` method
- New method `otp.datetime.to_operation`

### Fixed

- added missing per-tick script files to `onetick-py` distribution
- add support for selecting columns with regular expressions

### Changed

- order and naming of sections in documentation API reference
- raise exception when indexing state variables
- deprecated using tuples with name and type in `otp.Source.__getitem__`

## [1.82.0] - 2023-11-06

### Added

- docs and example with using function as a `query` parameter in `otp.run`
- added `otp.Source.Symbol.get()` method with ability to pass `default` parameter
- All methods of `_inspection.DB` class use `context` property of the class

### Fixed

- optimized getting last loaded date for the database

## [1.81.0] - 2023-10-30

### Added

- `pip install onetick-py[strict]` is now available for installing onetick-py with strict dependencies
- added `onetick-py` and OneTick versions to the exception message in `otp.run`
- `Source.write()`, `DB.add()` and `write_to_db()` now have `start`/`end` parameters for multiple dates writing.
- `otp.Column.cumsum()` function
- Parameter `inplace` for aggregations `apply()` method
- Parameter `overwrite_output_field` for aggregations

### Fixed

- fixed some documentation pages to not be platform-specific
- fixed unstable tests

### Changed

- default documentation page

## [1.80.0] - 2023-10-23

### Added

- `otp.Source.join_with_collection` function
- `check_schema` parameter for tick's get/set value functions
- added `protocol` and `resource` parameters for `otp.RemoteTS`

### Fixed

- Documentation upload and CI/CD requirements fixed
- compatibility issues with OneTick release 1.22 and 1.23
- method to check if OneTick feature is supported
- fix source's schema after applying order book aggregations
- compatibility issues with Python 3.7, 3.8 and 3.11

## [1.79.0] - 2023-10-16

### Added

- `otp.default_by_type` function
- `__repr__` implementations for onetick-py types

### Fixed

- adding `fields` to real schema in `.insert_tick()`

### Changed

- Switched cloud from **MiniC** to dev cloud
- Compatibility tests now use tags instead of branches

## [1.78.0] - 2023-10-10

### Added

- `.str.strptime` function (alias to `.str.to_datetime()`)

### Fixed

- do not run `windows` and `docs` tests on release creation pipeline
- fixed import error with `Literal` on python 3.7
- tests on compatibility with OneTick release 1.23
- fixed all warnings when building documentation, turned warnings into errors
- fixed the name of `timezone` parameter for most datetime accessor functions

### Changed

- more flexible `numpy` version requirements, 1.26.0 is now supported
- multiversion docs now also supports `master` branch version (published only on GitLab Pages)
- multiversion docs now based on tags instead of branches

## [1.77.0] - 2023-10-02

### Added

- `quote_char` parameter for `otp.CSV`
- support updating state variables in `otp.Source.update()` method
- `otp.format` function

### Fixed

- Fixed signature in documentation for some aggregations
- Added compare method for date difference object
- `pylint` job moved to `linters` stage
- some "out-of-order" jobs now depend on `trigger release`
- removed `__init__.py` file from project's root

### Changed

- make job `pages-multi-version` optional

## [1.76.0] - 2023-09-25

### Added

- an example of using ISN with OHLCV in Getting Started Guides
- `dtype` parameter for `get_long_value` and `get_string_value`
- Link to the video tutorial in the documentation
- Gitlab CI/CD variables to create release automatically in Jira project
- changelog linter, that fails when changelog have no changes in MR
- automatically generated compatibility tests in docs
- script for multiple version documentation building

### Fixed

- `get_value` can now return subclasses of string and integer
- different `otp.string` types can be used in `desired_schema` for `otp.DataSource`
- Fixed spelling in the documentation
- `None` is now equal to `otp.nan` when applying function
- `otp.dt` can be used for `otp.config['default_start_time']` and `otp.config['default_end_time']`

## [1.75.0] - 2023-09-18

### Added

- Linter for markdown files
- Support for inner functions in per-tick script

### Fixed

- Fixed using parameter `bucket_units` on old OneTick versions
- Fixed `trigger` pipelines in Gitlab CI
- Don't use `stack_info` parameter of EP objects to save stack trace.

### Changed

- `all_fields` parameter for `otp.agg` can now be set to `HighTick` or `LowTick` which allows to set the input field
- Getting database interval with database name as symbol, not `LOCAL::`

## [1.74.0] - 2023-09-11

### Added

- `otp.Symbol` now has `__getitem__` method which allows to set type of the symbol parameter
- `unit` parameter for `to_datetime()` method for `otp.Operation.str`
- `otp.Tick`: added parameter `bucket_units`

### Fixed

- `otp.CSV` now can be used with both file and symbol at the same time

### Changed

- `otp.by_symbol` `single_invocation` parameter default value is now `False`
- `otp.CSV` will be sorted by `timestamp_name` column
- `on` parameter for `otp.join()` can now be list of string

## [1.73.0] - 2023-09-04

### Added

- `otp.derived_databases` and parameter `derived` for `otp.databases`

## [1.72.0] - 2023-08-31

### Changed

- Change gitlab CI release pipeline, use automatic versioning

## [1.71.11]

### Fixed

- `otp.join` now doesn't add additional fields to the joined sources

## [1.71.10]

### Added

- `otp.agg.generic`: added `params` parameter to `.apply()` method, which allows to pass parameters to the aggregation function

## [1.71.9]

### Fixed

- support `otp.decimal`, `otp.uint`, `otp.short` and `otp.byte` in aggregations

## [1.71.8]

### Added

- `handle_escaped_chars` param in `otp.CSV()` to handle backslash escaped characters in CSV files.

## [1.71.7]

### Fixed

- it doesn't fail now when we run aggregation with `all_fields` in `[True, "last", "first", "high", "low"]`
  on data with changing schema

## [1.71.6]

### Fixed

- Fixed some compatibility with Python 3.7-3.8

## [1.71.5]

### Changed

- onetick import exception message and warnings logic

## [1.71.4]

### Added

- `otp.run` parameter `date`

## [1.71.3]

### Added

- `field_delimiters` parameter for `otp.CSV`

### Fixed

- `otp.CSV` now works correctly when `first_line_is_title=False` and `names` is set

## [1.71.2]

### Fixed

- Updated pandas version requirements for Python 3.10 compatibility

## [1.71.1]

### Fixed

- Use `onetick-init` project to use correct `onetick/__init__.py` file

## [1.71.0]

### Added

- `slice()` method for `otp.Operation.str`
- `[]` syntax for `otp.Operation.str.get`

## [1.70.3]

### Fixed

- Added `functools.cache` to `backports` for backward compatibility with python <= 3.8

## [1.70.2]

### Added

- default database now has `heartbeat_generator` CEP adapter and can be used with tick generator in CEP queries
- temporary files can be created with non-random names

## [1.70.1]

### Fixed

- Fixed `omd_dist_path()` function logic when `MAIN_ONE_TICK_DIR` variable is used

## [1.70.0]

### Added

- `date_trunc()` method for `otp.Operation.dt`
- `day_name()` method for `otp.Operation.dt`
- `day_of_month()` method for `otp.Operation.dt`
- `day_of_year()` method for `otp.Operation.dt`
- `hour()` method for `otp.Operation.dt`
- `minute()` method for `otp.Operation.dt`
- `month()` method for `otp.Operation.dt`
- `month_name()` method for `otp.Operation.dt`
- `quarter()` method for `otp.Operation.dt`
- `second()` method for `otp.Operation.dt`
- `week()` method for `otp.Operation.dt`
- `year()` method for `otp.Operation.dt`

## [1.69.0]

### Added

- support using `MAIN_ONE_TICK_DIR` environment variable
  to find the path to onetick python libraries instead of `PYTHONPATH`

## [1.68.1]

### Changed

- it is possible to pass any config variables into `otp.session.Config` via `variables`

## [1.68.0]

### Added

- `get()` method for `otp.Operation.str`
- `concat()` method for `otp.Operation.str`
- `insert()` method for `otp.Operation.str`
- `first()` method for `otp.Operation.str`
- `last()` method for `otp.Operation.str`
- `startswith()` method for `otp.Operation.str`
- `endswith()` method for `otp.Operation.str`

### Changed

- `find()` method for `otp.Operation.str` has new parameter `start`

## [1.67.1]

### Fixed

- OneTick release version and build date now both used to check compatibility for specific features,
  to avoid issues with older OneTick releases patched.

## [1.67.0]

### Added

- ability to specify a db on the CSV source to determine a destination where the csv file will be processed

## [1.66.0]

### Added

- `erase()` method for `otp.stat.tick_list`

## [1.65.5]

### Fixed

- using `.apply(int)`, `.apply(otp.nsectime)` and `.dt.day_of_week()` methods
  in per-tick script

## [1.65.4]

### Fixed

- fixed docs for str accessors

## [1.65.3]

### Fixed

- `otp.CSV()` now supports specifying path with symbol name

## [1.65.2]

### Added

- `auto_increment_timestamps` parameter for `otp.CSV` source

## [1.65.1]

### Fixed

- `otp.CSV()` now supports big int values without trimming it.
- str to int column change now also supports big int values without trimming it.

## [1.65.0]

### Added

- `max_back_ticks_to_prepend` parameter for `otp.DataSource`
- `where_clause_for_back_ticks` parameter for `otp.DataSource`

## [1.64.2]

### Fixed

- added support for passing symbol time to `join_with_query` as string

## [1.64.1]

### Changed

- `otp.corp_actions` changes `adjustment_date_tz` value to `GMT` if `adjustment_date` has YYYYMMDD format

## [1.64.0]

### Added

- `input` property of tick inside script

## [1.63.2]

### Added

- New parameters for `Source.join_with_query`:
  - `default_fields_for_outer_join`
  - `symbol_time`
  - `concurrency`

### Changed

- `join_with_query` now by default uses timezone of the main query and not `otp.tz`

### Fixed

- nanosecond-precision query parameters are now being passed to joined query without precision loss

## [1.63.1]

### Fixed

- do not add pseudo fields in schema when calling `.rename()` with regexp

## [1.63.0]

### Added

- `otp.decimal`

### Fixed

- detecting new types in `_inspection.DB.schema()`

## [1.62.1]

### Fixed

- `otp.CSV`: fix default timestamps with nanoseconds

## [1.62.0]

### Added

- `otp.math.floor` function

## [1.61.2]

### Fixed

- `otp.Source.rename` now works with regexp

## [1.61.1]

### Fixed

- `otp.math.max/min` now works in `otp.script`

## [1.61.0]

### Added

- added `otp.agg.variance` aggregation

## [1.60.8]

### Changed

- enable `include_memdb` for `DB.tick_types()` method

## [1.60.7]

### Fixed

- aggregations with `Operation` in `group_by` parameter now return `GROUP_{i}` column

## [1.60.6]

### Fixed

- `dtype` parameter for `otp.CSV` now works correctly

## [1.60.5]

### Fixed

- `compute` with `all_fields=True` now works correctly for aggregations on time based columns

## [1.60.4]

### Fixed

- `tick_set.find()` now allows to pass string columns of different lengths as key value
- `otp.Once` renamed to `otp.once`

## [1.60.3]

### Fixed

- do not require `numpy` directory on latest OneTick builds

## [1.60.2]

### Fixed

- `otp.varstring` now works with `otp.state.tick_set`

## [1.60.1]

### Removed

- parameter `add_default_db` for `otp.Locator`
- parameter `add_default_db_to_locator` for `otp.Config`

## [1.60.0]

### Added

- `columns` and `ignore_columns` parameters for `otp.Source.add_prefix` and `otp.Source.add_suffix` methods

## [1.59.3]

### Added

- parameter `add_default_db` for `otp.Locator`
- parameter `add_default_db_to_locator` for `otp.Config`

## [1.59.2]

### Added

- parameter `num_threads` for method `otp.Source.process_by_group()`

## [1.59.1]

### Changed

- `running` flag of `otp.run()` now applies only to main graph, and not to any sub graphs
  (`symbols`, `join_with_query`, etc.)

## [1.59.0]

### Added

- slicing select for ticks with `otp.Source.__getitem__()`

## [1.58.9]

### Changed

- simplified `nsectime` to int conversion

## [1.58.8]

### Fixed

- `otp.run()` warnings regarding OntTick build version is now show both installed and required versions

## [1.58.7]

### Added

- testing script intended to be used in OneTick build release process

## [1.58.6]

### Added

- `otp.Source.show_symbol_name_in_db()`

## [1.58.5]

### Added

- spell-checking when building documentation

## [1.58.4]

### Fixed

- `TickSet.find` now works without `key_fields` when `throw=True`

## [1.58.3]

### Added

- `otp.Source.mean`

### Changed

- deprecated datetime subtraction

## [1.58.2]

### Added

- `otp.ulong`
- `otp.uint`
- `otp.short`
- `otp.byte`

## [1.58.1]

### Added

- support for multiple `pandas` versions for python 3.9 and 3.11

## [1.58.0]

### Added

- support using `otp.Once` in per-tick script

## [1.57.5]

### Fixed

- iteration through list of strings in per-tick script now works correctly

## [1.57.4]

### Added

- support `otp.Operation` methods in `ExpressionDefinedTimeOffset`

## [1.57.3]

### Fixed

- `ott.string` now have default length None to distinguish from `string[64]` to avoid broken conversion in `table()`

## [1.57.2]

### Fixed

- `TickList.push_back()` now changes tick list's schema

## [1.57.1]

### Changed

- improving docs for `otp.config` and `Configuration` pages

## [1.57.0]

### Added

- get/set functions for ticks in per-tick script to use with value or `Operation`
- get/set functions for `TickSequenceTick` and `DynamicTick` can also use `Operation`

## [1.56.2]

### Changed

- setting configuration for `getting_started` docs

## [1.56.1]

### Added

- `otp.DB`: `db_raw_data` and `db_feed` parameters
- `otp.DB`: `raw_data` and `feed` properties

## [1.56.0]

### Added

- `subset` parameter added to `otp.Source.dropna()`

## [1.55.10]

### Fixed

- `TickSet` now support `.erase()` with one or two `TickSequenceTick`
- `TickSequenceTick` updates schema when used in `TickSet.find()`

## [1.55.9]

### Changed

- speed up adding multiple databases, locators, acls to the session

## [1.55.8]

### Changed

- support updating source schema with complex types and objects

## [1.55.7]

### Fixed

- `otp.Ticks` dataframe modification

## [1.55.6]

### Changed

- deprecate `otp.Source.to_df()` and `otp.Source.__call__()`

## [1.55.5]

### Fixed

- allow `otp.agg.option_price` to be used in `otp.Source.agg`

## [1.55.4]

### Fixed

- while deducing schema use last day with *selected* tick type

## [1.55.3]

### Added

- support for `numpy` data types in some operations

## [1.55.2]

### Changed

- use PRESORT in `otp.DataSource` with multiple symbols

## [1.55.1]

### Added

- `otp.DataSource` strict schema policies

## [1.55.0]

### Added

- support reading onetick-py config from `OTP_DEFAULT_CONFIG_PATH` env variable
- support `OTP_SHOW_STACK_INFO` env variable

## [1.54.2]

### Added

- `while` statement in per-tick script

## [1.54.1]

### Fixed

- changing column type from `float` to `int` now works correctly

## [1.54.0]

### Added

- added possibility to log symbol in `otp.run` through `OTP_LOG_SYMBOL`

## [1.53.4]

### Fixed

- set `otp.config.show_stack_info` to `False` by default

## [1.53.3]

### Changed

- changed changelog formatting

## [1.53.2]

### Changed

- `otp.Source.time_filter` default timezone parameter

## [1.53.1]

### Fixed

- warning if database with the same case-insensitive name is added

## [1.53.0]

### Added

- `otp.Source.modify_query_times`
- `otp.Source.time_interval_shift`
- `otp.Source.time_interval_change`

## [1.52.0]

### Added

- `otp.config.show_stack_info`
- stack trace for all `onetick.query` EPs

## [1.51.0]

### Added

- added `otp.TestSession` which sets up default values

## [1.50.2]

### Fixed

- fixed `SharesOutstanding` timestamp out of bounds

## [1.50.1]

### Added

- parameter `max_expected_ticks_per_symbol` for `otp.run`
- `otp.config.max_expected_ticks_per_symbol`

## [1.50.0]

### Added

- parameter `password` for `otp.run`
- `otp.config.default_auth_username`, `otp.config.default_password`

## [1.49.1]

### Fixed

- `otp.CSV`: support fixed-length strings and varstrings

## [1.49.0]

### Added

- Support multiple endpoints in the `servers.RemoteTS` entity
- Introduced `LoadBalancing` for `RemoteTS`
- Introduced `FaultTolerance` for `RemoteTS`

## [1.48.4]

### Changed

- rename `start_time` and `end_time` parameters to `start` and `end`
  in `join_with_query` and other places

## [1.48.3]

### Fixed

- Sphinx documentation
- Added ability import `otp.corp_actions` as an alias for `otp.functions.corp_actions`
- `policy` parameter in the `join_by_time` takes lower case values

## [1.48.2]

### Fixed

- fix `inplace` parameter for `otp.Source.insert_tick()`

## [1.48.1]

### Fixed

- custom start and end time in evaluated first-stage queries

## [1.48.0]

### Added

- Added support for Python 3.7-3.10, `onetick.py.backports` module added for all common
  backward compatible imports.

## [1.47.0]

### Added

- `otp.agg.num_distinct`

## [1.46.1]

### Fixed

- remove null-characters when converting string to varstring

## [1.46.0]

### Added

- `otp.Source.insert_tick()` method

## [1.45.4]

### Changed

- do not create temporary directory in case `otp.DB` locations are specified by user

## [1.45.3]

### Added

- sort() function for tick lists

## [1.45.2]

### Added

- changelog page in docs

## [1.45.1]

### Added

- `otp.Source.dump`: message `<no data>`

## [1.45.0]

### Added

- ranking aggregation class and method

## [1.44.0]

### Added

- exposed `otp.OneTickLib`
- `otp.OneTickLib().set_authentication_token()` method

## [1.43.2]

### Fixed

- make generic aggregation tests pass on newest onetick builds again

## [1.43.1]

### Changed

- `otp.DataSource`: search days back to find schema with tolerant schema policy

### Fixed

- `last_not_empty_date()` method will not go beyond locator boundaries

## [1.43.0]

### Added

- more `nsectime`/`msectime` conversions in column's `.apply()` method

## [1.42.0]

### Added

- convert from/to `otp.string` in column's `.apply()` method

## [1.41.5]

### Fixed

- make generic aggregation tests pass on newest onetick builds

## [1.41.4]

### Fixed

- `Column.str.to_datetime()` now preserve nanoseconds when accessor is the same column

## [1.41.3]

### Fixed

- merging string and varstring column
- concatenating varstring with string

## [1.41.2]

### Changed

- join_by_time now checks types in `on` parameter columns only if `check_schema` is `True`

## [1.41.1]

### Fixed

- node is not checked if result is empty

## [1.41.0]

### Added

- `Source.time_filter()` method added to filter ticks by time

## [1.40.1]

### Backward incompatible change

- join_by_time now raise TypeError if `on` parameter is set and sources have different types of this column

## [1.40.0]

### Added

- added `otp.agg.option_price` aggregation

## [1.39.0]

- support callback mode in `otp.run`
- `otp.CallbackBase` class

## [1.38.0]

### Added

- support `remote://` in `otp.query`

## [1.37.1]

### Fixed

- Fix `otp.run()` error when query property repository not initialized

## [1.37.0]

### Added

- added `OTP_DEFAULT_FAULT_TOLERANCE` config value

## [1.36.2]

### Fixed

- `otp.agg.generic` improvements
  - remove `all_fields` parameter from docs
  - save aggregation sub-query to the same file

## [1.36.1]

### Fixed

- updated requirements.dev.txt with strict versions of sub-dependencies to shorten the installation time

## [1.36.0]

### Added

- added `CORRELATION` aggregation

## [1.35.3]

### Added

- `otp.agg.stddev` now supports `biased` parameter

## [1.35.2]

### Fixed

- float to string conversion in float accessor methods

## [1.35.1]

### Changed

- removed timezone warning when creating stubs database locator

## [1.35.0]

### Added

- parameter `default` for `otp.Operation.map()`

## [1.34.0]

### Changed

- ignore unsupported field types when inspecting database

### Fixed

- detect `otp.varstring` type when reading database

## [1.33.0]

### Added

- `yield` and `copy_tick()` for per-tick script

## [1.32.0]

### Added

- `otp.raw` class for specifying raw OneTick expressions

### Fixed

- converting string literals to OneTick syntax

### Backward incompatible change

- hack with string surrounded by double quotes is removed

## [1.31.0]

### Added

- `otp.oqd.sources.SharesOutstanding` source with is actually `OQD_SOURCE_SHO EP`

## [1.30.0]

### Changed

- removed `otp.utils.memoize`
- `otp.utils` module refactoring

## [1.29.0]

### Added

- `otp.string[...]` and a shortcut for it `otp.varstring` which represent `varstring`

## [1.28.0]

### Fixed

- `otp.join_by_time` now can be used in `otp.agg.generic`

### Added

- parameter `use_rename_ep` for `otp.join_by_time`

## [1.27.1]

### Fixed

- `pandas.DataFrame` in `otp.Ticks()` now supports `nan` on Windows

## [1.27.0]

### Added

- `add_prefix` and `add_suffix` functions to `otp.Source`

## [1.26.0]

### Changed

- use `TmpDir` instead of `GeneratedDir` to create database directories

### Added

- parameter `rel_path` for `TmpDir`

### Backward incompatible change

- other `TmpDir` parameters are changed to be keyword-only

## [1.25.0]

### Added

- otp.agg.generic

## [1.24.0]

### Fixed

- automatically set default db/symbol/times for queries where possible

## [1.23.0]

### Fixed

- `inplace` parameter for `Source.update()`

### Backward incompatible change

- default `inplace` logic changed to False

## [1.22.0]

### Added

- added `otp.Operation.__round__` method
- fixed docs

## [1.21.2]

### Fixed

- `float('nan')` behaves the same way as `otp.nan`

## [1.21.1]

### Changed

- `otp.functions.corp_actions()` parameter `adjustment_date` now supports
  `otp.datetime`, `otp.date`, `datetime.date`, `datetime.datetime`, `str` types

## [1.21.0]

### Added

- `inplace` parameter for `Source.write()`
- support using `otp.Column`s as `symbol` and `tick_type` parameters
- added all OneTick parameters to `Source.write()`

### Fixed

- fix wrong merge logic for `Source.write()` when `propagate_ticks=True`

### Changed

- unify `DB.add()`, `DB.put()`, `Source.write()`, `otp.db.write_to_db()` as much as we can

## [1.20.0]

### Added

- added ability to use `pandas.DataFrame` in `otp.Ticks()`

## [1.19.0]

### Fixed

- removed overriding of `tempfile` module's private function

### Added

- `mkstemp`, `mkdtemp`, `ONE_TICK_TMP_DIR` functions in `otp.utils`

### Changed

- refactoring of tmp files and database creation logic
- remove usage of `SKIP_NAME_GENERATION` and `DISABLE_COOL_TMP_NAMES` environment variables

## [1.18.1]

### Fixed

- disable emulation in inner .apply() functions

## [1.18.0]

### Backward incompatible change

- deleted ability to use built-in `min`/`max` functions for `otp.sources`
- now only `otp.math.min`/`otp.math.max` should be used for `otp.sources`
If there are any usage of built-in `min`/`max` functions for `otp.sources`,
please, replace them with `otp.math.min`/`otp.math.max`.

## [1.17.4]

### Fixed

- fixed path checking for PYTHONPATH

## [1.17.3]

### Fixed

- calling inner functions with tick fields in `.apply()` method

## [1.17.2]

### Fixed

- protected from accidentally adding new attributes to the `otp.config` object

## [1.17.1]

### Added

- `large_ints` parameter for `First` and `Last` aggregations

### Fixed

- support `nsectime` by `First` and `Last` aggregations

## [1.17.0]

### Added

- `OTP_DEFAULT_LICENSE_DIR` and `OTP_DEFAULT_LICENSE_FILE` configuration
environment variables to set the location of license files

## [1.16.5]

### Fixed

- per-tick script local variables

## [1.16.4]

### Fixed

- state variables can be updated with simple values

## [1.16.3]

### Fixed

- nanoseconds precision is not lost when using min/max functions
with datetime arguments

## [1.16.2]

### Added

- `otp.config.default_concurrency`
- `otp.config.default_batch_size`

### Changed

- default concurrency is set to the number of cores on the machine,
   previously concurrency was disabled

## [1.16.1]

- Jupyterlab-snippets support added with command `jupyter onetick_snippets jupyterlab_snippets`

## [1.16.0]

### Changed

- change default config parameters
  - default timezone is changed from `EST5EDT` to local
  - default start time, end time, database and symbol are removed
- `otp.DEFAULT_*` variables are fixed, but deprecated

## [1.15.12]

### Added

- `Columns.map()` function that mimics `pandas.Series.map()`

## [1.15.11]

### Fixed

- forbid using pseudo-fields in .table()

## [1.15.10]

### Fixed

- support using operations with `_TIMEZONE` in `otp.join`
- support using `datediff` operation in `otp.join`
- bug with some columns filtered out with `otp.join`

## [1.15.9]

### Fixed

- change default timezone for datetime functions

## [1.15.8]

### Added

- `otp.functions.save_sources_to_single_file()`

## [1.15.7]

### Fixed

- don't use cache in `otp.DB.tick_types()`

## [1.15.6]

### Added

- `otp.meta_fields` class
- `otp.expr` class
- `otp.Operation.expr` property
- support for using `otp.expr` in `otp.DataSource` parameter `back_to_first_tick`

## [1.15.5]

### Added

- `otp.RefDB` class

## [1.15.4]

### Fixed

- support `otp.string` in tick sequences functions

## [1.15.3]

### Fixed

- `otp.DB.last_date` now returns first no empty date

### Added

- `otp.DB.last_not_empty_date()` function

## [1.15.2]

### Changed

- delete legacy code with `_CompareEmulator`

## [1.15.1]

### Changed

- `numpy` version from 1.19.5 to 1.23.0

## [1.15.0]

### Added

- `otp.date` and `otp.datetime` timezone awareness
- support `otp.date`, `datetime.date` and `pandas.Timestamp` in `otp.Source.table()`
- support `otp.date` as absolute time in `otp.Ticks` and `otp.Tick`
- support creating `otp.datetime` with `otp.date`
- support adding `otp.date` as column
- removed `otp.types.datetime2str`, added `otp.types.datetime2expr` instead

### Fixed

- fix `ott.time2nsectime()` function

## [1.14.48]

### Added

- support empty return in per-tick script

## [1.14.47]

### Added

- `otp.oqd.sources.OqdSourceDes` and `otp.oqd.funcs.corp_actions`

## [1.14.46]

### Added

- support many tick types in `otp.DataSource`

## [1.14.45]

### Changed

- updated `onetick-py-test==1.1.30`

### Fixed

- `otp.config`: added `.get()` method
- `otp.Source.write()`: new default value for `date` parameter

## [1.14.44]

### Fixed

- default end time is next day's midnight
- `otp.date`: `__add__` and `__sub__` methods

## [1.14.43]

### Fixed

- `otp.DB` will raise exception when ticks do not fall into the time range determined by DAY_BOUNDARY_TZ

## [1.14.42]

### Fixed

- sources.CSV now support relative CSV file path located inside CSV_FILE_PATH folder

## [1.14.41]

### Added

- `otp.state.tick_list`, `otp.state.tick_set`, `otp.state.tick_deque`
- `otp.Source.execute`
- support iterating over tick sequences in per-tick script

## [1.14.40]

### Added

- `otp.remote` decorator for apply functions used in ray remote context

## [1.14.39]

### Added

- `otp.oqd` module with `oqd` event processors and sources

## [1.14.38]

### Added

- `otp.by_symbol` function that allows to split query by symbols and use result with unbound symbols
- added ability to pass in the `otp.CSV` file buffer created using the `otp.utils.file` in remote execution environment

## [1.14.37]

### Changed

- Improving `otp.CSV` types inspection, added timestamp field argument,
  mimics pandas arguments `names=`, `converters=` and `dtype=`.

## [1.14.36]

### Fixed

- schema dates lookup interval increased to get 5 five days back
- fixed operation with Nanoseconds and constant `dt` objects

## [1.14.35]

### Fixed

- fix apply() method with brackets, change lambda token parser

## [1.14.34]

### Fixed

- min/max aggregation object modification when used on datetime columns

## [1.14.33]

### Changed

- Changed method for converting python code to OneTick's per-tick script and case() function
- Improving performance of .script() and .apply() methods in more complex functions

## [1.14.32]

### Changed

- All defaults moved to `otp.config.[...]` . E.g. `otp.config.tz` or `otp.config.default_start_time`
- Options can also be accessed like `otp.config['tz']`
- `otp.DEFAULT_[...]` are preserved for backward compatibility; `otp.Source.DEFAULT_[...]` no longer exist.

### Added

- `config.default_symbology` to set default symbology for all databases created by `onetick.py`

## [1.14.31]

### Added

- `otp.DataSource`: `keep_first_tick_timestamp` parameter

## [1.14.30]

### Changed

- rearranged docs to exclude sphinx and jupyter dependencies from the dist package

### Added

- script that helps to pack artifacts for OT integration

## [1.14.29]

### Added

- `otp.Source` methods: `ob_snapshot()`, `ob_snapshot_wide()`, `ob_snapshot_flat()`

## [1.14.28]

### Added

- `time_series_type` parameter for First, Last, FirstTime, LastTime, FirstTick, LastTick, HighTime, LowTime

### Fixed

- inheritance parameters for `HighTick`, `LowTick`, `HighTime`, `LowTime`

## [1.14.27]

### Changed

- `onetick.docs` moved into the `onetick.py.docs`

## [1.14.26]

### Added

- `otp.MultiOutputSource` class to create queries that return multiple outputs

## [1.14.25]

### Added

- Possibility pass timezone with Operation type to `str` and `dt` accessor for `to_datetime` and `strftime` method

## [1.14.24]

### Added

- Sphinx doc: added overview, getting started and concepts into the doc. Added documentation to the data inspection api.
- `otp.config['context']` config variable

### Removed

- `pytz` dependency
- old version of the data inspection mechanism

## [1.14.23]

### Added

- `otp.coalesce()`

## [1.14.22]

### Added

- fix adding string columns in `otp.Source.script()`

## [1.14.21]

### Added

- `otp.utils.TmpFile` now use `__del__()` instead of `weakref.finilize()`

## [1.14.20]

### Added

- Aggregations: `otp.agg.ob_snapshot`, `otp.agg.ob_snapshot_wide`, `otp.agg.ob_snapshot_flat`
- Sources: `otp.ObSnapshot`, `otp.ObSnapshotWide`, `otp.ObSnapshotFlat`

## [1.14.19]

### Added

- `otp.SymbologyMapping` source
- symbology and `show_original_symbols` parameters for `otp.Symbols`

## [1.14.18]

### Added

- Environment variable `OTP_SKIP_OTQ_VALIDATION` now force to skip `__validate_onetick_query_integration()`

## [1.14.17]

### Fixed

- `otp.DB`: try to get schema from db again without using cache if we failed to get it the first time

## [1.14.16]

### Added

- argument `all_fields` of `otp.agg()` could set policy to choose tick for all fields: first, last, high, low.

## [1.14.15]

### Fixed

- `otp.cut`, `otp.qcut`: raise exception when number of labels is not equal to number of bins
- handle case when using cut functions on one field several times

## [1.14.14]

### Fixed

- added logic to support changed behaviour of newer OneTick builds with respect to timezones passed to `otp.run()`

## [1.14.13]

### Fixed

- `keep_everything_generated` property for the classes `TmpFile`, `TmpDir` and `GeneratedDir` to control tmp file cleanup

## [1.14.12]

### Fixed

- 'members' default `autodoc` option

## [1.14.11]

### Added

- ability to use `otp.Operation` in aggregations

## [1.14.10]

### Added

- docs for datetime classes

### Fixed

- `otp.Year`, `otp.Quarter`, and `otp.Month` datetime offsets

## [1.14.9]

### Added

- added documentation

### Fixed

- fixed wrong documentation substitution for aggregations methods

## [1.14.8]

### Changed

- improved doctest for `apply` and `__getitem__` on the Source
- added ability to use external queries from the `doctest_resources` subfolder

## [1.14.7]

### Added

- `otp.math.pi()` function
- improved docs for all `otp.math` functions

## [1.14.6]

### Added

- `Source` slice `data[['x', 'y']:]` now works like handy shortcut for `Source.table(strict=False)`

## [1.14.5]

### Changed

- `otp.Source.high_time` and `otp.Source.low_time` deprecated (use `otp.agg.high/low_time` instead)

### Fixed

- Field collision in `Source.agg()` with `all_fields=True` and non-empty `group_by`

## [1.14.4]

### Added

- `otp.DataSource` (or `otp.Custom`) now have `back_to_first_tick` parameter.

## [1.14.3]

### Fixed

- Proper error message for using full tick aggregation on column.

## [1.14.2]

### Added

- new optional flag -i (--name-info) for `jupyter onetick_snippets` (shows snippets name tree)

## [1.14.1]

### Fixed

- Fix updating datetime columns with datetime functions

## [1.14.0]

### Fixed

- Inconsistency in UPDATE_FIELD EP changing column type with datetime functions

## [1.13.0]

### Changed

- API documentation structure changed

## [1.12.1]

### Fixed

- Warning instead of an error when using source with improper schema as an FSQ
- Restored support for passing _SYMBOL_TIME query parameter to "join_with_query" method

## [1.12.0]

### Fixed

- objects designed to be protected moved to protected space

## [1.11.1]

### Added

- ability to use absolute time instead of offset in `otp.Tick()` and `otp.Ticks()`
- ability to pass the whole source as a list of symbol parameters in `join_with_query()`

## [1.11.0]

### Added

- Added `otp.functions.cut()` and `otp.functions.qcut()`, that mimics `pandas.cut()` and `pandas.qcut()`

## [1.10.3]

### Fixed

- nodes history rebuilding in `otp.Source.deepcopy()` method

## [1.10.2]

### Fixed

- Method `tick_types` in inspection `DB` class can accept None date then assign `self.last_date` if it is None,
  `self.last_date` also can be None and when we pass arguments to `otp.run`
  we create 'end' via `date + timedelta(days=1)` that triggered Exception.
  Now it passes 'utils.adaptive' start and end in the case when date is finally None.

## [1.10.1]

### Added

- `_TIMEZONE` meta column of the `otp.Source` object

### Fixed

- If you use `otp.run()` with a query as graph file and symbols as `otp.Source`, timezone is now being passed
  to the symbols query correctly

## [1.10.0]

### Added

- `jupyter-onetick_snippets` executable - configure snippets for `nbextension` `snippets/snippets_menu`

## [1.9.0]

### Added

- Added scripts which allows running QueryDesigner.exe on Windows for test case and dashboard on Linux

## [1.8.0]

### Added

- math module with OneTick math methods there

## [1.7.1]

### Fixed

- Datetime fields are now correctly converted to integers when passed as filter conditions

## [1.7.0]

### Added

- Support `numpy==1.19.5` for python3.9

## [1.6.1]

### Changed

- `otp.agg.<aggregation>` is now function and return aggregation instance

### Added

- `Median` aggregation
- `Source.agg` will now work if `running=False` and `all_fields=True` - first tick in bucket used for `all_fields`
- method `apply` for all aggregations that allows to apply aggregation to `Source`
- Source methods `high`, `low`, `first`, `last`, `high_time`, `low_time`, `distinct`
  now support all parameters from relevant aggregation

## [1.6.0]

### Changed

- all calls to `_Source` changed to `self.__class__` where possible
- `copy()` function now create subclass instance instead of `_Source`
- `output_type_index` parameter for `merge()`, `join()`, `join_by_time()` and `apply_query()`
- all standard `otp.Source` subclasses now support `node` and `**kwargs` parameters for `__init__()`
- from this point all subclasses of `otp.Source` *must* support `node` and `**kwargs` parameters for `__init__()`

### Fixed

- fixed problem with `otp.Source.drop_columns()` deleting custom properties

## [1.5.24]

### Fixed

- `otp.eval(source_func, symbol)` now passes the whole symbol
  (`_SymbolParamSource()`) object to the `source_func` and not only symbol name
- Internal graphs for sources and `eval` used as merge symbols are now stored
  in the main query file and not as separate files
- `Source.distinct()` now aligns schema properly to have only key fields after the aggregation

## [1.5.23]

### Changed

- if `keep_timestamp==False` source aggregations won't add any reordering
- source aggregation `add_sort` parameter deprecated and has no effect anymore.
- if `keep_timestamp==True` source aggregations won't add redundant reordering and will drop `TICK_TIME` field

## [1.5.22]

### Fixed

- pass all parameters from `otp.run` to `otq.run`

## [1.5.21]

### Fixed

- The .update method works properly with string constants as values, and starts to return a resulting object

## [1.5.20]

### Fixed

- Fixed passing milliseconds with proper conversion into the constructor of `otp.nsectime` to support backward compatibility

## [1.5.19]

### Added

- support setting nanosecond constant to a column

## [1.5.18]

### Fixed

- allowed to be `otp.Symbols` without specified db, helpful when databases comes as symbols from symbols flow
- `Source.count()` failed in case of no ticks

## [1.5.17]

### Added

- `otp.Source.deepcopy()` method and `otp.Source.copy(deep)` parameter

### Changed

- `_NodesHistory` class refactoring: saving history as classes, not as closures

## [1.5.16]

### Fixed

- source columns after `join_with_query(prefix=...)`

## [1.5.15]

### Added

- `distinct` to support list of keys

## [1.5.14]

### Added

- `day_of_week` function for datetime accessor

## [1.5.13]

### Added

- `otp.agg.max` and `otp.agg.min` `time_series_type` parameter

### Fixed

- parameter iteration logic for `_Aggregation` class
- several typos in other aggregation's parameters

## [1.5.12]

### Fixed

- `otp.Custom` won't fail if wore than one db passed as symbol param

## [1.5.11]

### Fixed

- `otp.Symbols(..., keep_db=False)` worked incorrectly when symbol name contained a colon

## [1.5.10]

### Added

- OTP\_BASE\_FOLDER\_FOR\_GENERATED\_RESOURCE env variable that control where to save generated resources

## [1.5.9]

### Changed

- Removed marking of nan values as special

## [1.5.8]

### Added

- `where` parameter for `join_with_query`

## [1.5.7]

### Added

- `LocalCSVTicks` function, to read ticks from local csv file and create ticks object from them

## [1.5.6]

### Added

- Support *dateparts* in the `Ticks` `offsets`, i.e. `Hour`, `Nano`, `Minute`, but without expressions

## [1.5.5]

### Added

- Ability to pass parameters to `eval`
- `otp.Source` filtering supports `eval`
- `_DBNAME` meta field
- `_TICK_TYPE` meta field

## [1.5.4]

### Fixed

- the `.table` does not extend schema with new columns if `strict=False`

## [1.5.3]

### Added

- `keep_fields_not_in_schema` option for the `otp.functions.join()`
  preserves fields from sources that were not in the schemas

## [1.5.2]

### Added

- Support for CEP queries
- Added the `.write()` method on the source
- Added the `.count()` method on the source that returns number of ticks
- Support the FLOAT type for return OneTick schema

## [1.5.1]

### Added

- Ability to pass `pd.DataFrame` as a symbol list to `otp.run()` and `Source.to_otq()`

### Changed

- run.sh script that is generated for test cases now uses explicitly passed port by default and not `MAIN_TS_PORT`

### Fixed

- Fixed timezone conversions in database inspection
- Fix setting `day_boundary_tz` on database creation

## [1.5.0]

### Added

- Ability to pass expressions in the `group_by` parameter for aggregations
- Introduced the `otp.DEFAUT_TZ`, `otp.DEFAULT_START_TIME`, `otp.DEFAULT_END_TIME`, `otp.DEFAULT_DB`
- They can be configured using the corresponding env variables:
  `OTP_DEFAULT_TZ`, `OTP_DEFAULT_START_TIME`, `OTP_DEFAULT_END_TIME`, `OTP_DEFAULT_TZ`
- The `otp.DEFAULT_TZ` is set to EST5EDT by default, and `OTP_DEFAULT_TZ`  controls every place that works with timezones
- Exposed the `otp.databases()` function that returns all available databases,
  where each database has corresponding methods for getting available tick types, symbols, dates with data and schema
- Reworked mechanic of schema deducing, currently it is based on the `otp.databases()`:
  it does not use symbols under the hood anymore, that radically speeds up the algorithm and makes it timezone agnostic
- Added ability to pass callables as a query for the `otp.run`.
  It supports functions and methods that have only single parameter that reflects upcoming symbol.
- Added ability to pass `otp.dt` in the join with query

## [1.4.20]

### Fixed

- Fix bool type for multiple ticks in `otp.Ticks`.

## [1.4.19]

### Added

- Add ability to use flexible buckets for `Source.agg` method.

## [1.4.18]

### Added

- `keep_timestamp` parameter for "first", "last", "high" and "low" functions of Source class.

## [1.4.17]

### Added

- Added names for Source objects. Names are used for display purposes when saving queries to disk.
- Ability to save all sub-queries (JWQ, `eval`, etc) to the same `.otq` file when running a query
- `get_query_parameter_list` function of the query inspector

### Changed

- `Source` objects are now always saved to disk as `.otq` files when running.
- `to_graph` function of the `Source` class is now deprecated for complex queries

## [1.4.16]

### Added

- Add `tw_average` aggregation

## [1.4.15]

### Fixed

- Fixed ability to omit the `n_bytes` parameter in the `str.substr`

## [1.4.14]

### Added

- Add `same_size` option to join sources with same size directly

## [1.4.13]

### Fixed

- Fixed HighTime, LowTime aggregations can now accept column name

## [1.4.12]

### Added

- Add `all` option in join function

## [1.4.11]

### Fixed

- Remove using the onetick.test.data module in tests

## [1.4.10]

### Changed

- Remove unnecessary packages from the requirements.txt

## [1.4.9]

### Added

- Possibility to specify `db_locations` in `db.DB` with derived databases

## [1.4.8]

### Added

- Double check initialization of OneTickLib in the `otp.run`

## [1.4.7]

### Added

- Overloaded `>>` and `>>=` operators for sources that duplicate `Source.sink()` method

## [1.4.6]

### Fixed

- Problem related in the `get_schema` that could return no results due the timezone

## [1.4.5]

### Changed

- Ability to pass schema deduction policy to `get_schema()`

## [1.4.4]

### Fixed

- `Custom()` now works correctly with empty actual schema

## [1.4.3]

### Changed

- Changed hardcoded date to 2030

## [1.4.2]

### Changed

- Set onetick-lib version to 1.0.4

## [1.4.1]

### Added

- Ability to call custom onetick script

## [1.4.0]

### Changed

- Rename `Source.join` method to `unite_columns`.

## [1.3.118]

### Changed

- Removed warnings from `otp.utils`, they seems to happen quite often and annoying for end users
- Moved `onetick.test` package to the 1.1.26

## [1.3.117]

### Changed

- Migrate from `pytz` to `dateutil` in inner usages.

## [1.3.116]

### Added

- `Source.join` method for joining several columns into the one string.

## [1.3.115]

### Added

- `date` method to `dt` accessor, for date extraction from time fields

### Fixed

- Fields and state variable now can be initialized with `otp.date`

## [1.3.114]

### Fixed

- `bucket_time` argument to `otp.Tick`.

## [1.3.113]

### Fixed

- `min` and `max` aggregations are now working at the same time on `nsectime` fields.

## [1.3.112]

### Added

- `start_time_expression` and `end_time_expression` arguments to `otp.run`
- `start_time_expression`, `end_time_expression` and `query_param` arguments to `Source.to_df`

### Changed

- `time2nsectime` function moved to `otp.types` module

## [1.3.111]

### Added

- `DISABLE_COOL_TMP_NAMES` env allowing to disable patching of tmp file name
   generator with `coolname` implementation

## [1.3.110]

### Fixed

- `nsectime` columns loose nanoseconds after the `min` and `max` aggregations

## [1.3.109]

### Added

- `stddev` aggregation

## [1.3.108]

### Changed

- pandas requirement change to 1.1.4.

## [1.3.107]

### Added

- `__repr__` method to `otp.datetime` class.

## [1.3.106]

### Fixed

- Problems with initialization and arithmetic operations written in science notation

## [1.3.105]

### Added

- double comparison by `cmp` and `eq` methods

## [1.3.104]

### Added

- `otp.run` method
- Symbol parameters now accessible via `Source.Symbol.<parameter_name>`
- `otp.date` object now can be created from `otp.datetime`, pandas.Timestamp or datetime.datetime
- `date` method to `otp.datetime` object
- `to_str` method to `otp.date` object
- `otp.datetime` now can be created from another `otp.datetime` object

### Fixed

- time-value column comparison

## [1.3.103]

### Fixed

- `Symbol` argument of `otp.Ticks` now fills `_SYMBOL_NAME` value.

## [1.3.102]

### Fixed

- comparison of time column with time constants is now possible

## [1.3.101]

### Fixed

- the `group_by` method applied a pin for a single output that broke binding with nested queries

## [1.3.100]

### Added

- method of class can be used as parameter of `otp.eval`

## [1.3.99]

### Fixed

- `dump` method now add labels if columns parameter was specified

### Added

- callback parameter to `dump` method to preprocess data before dumping
- `dump` method now accept string as columns parameter in such case only one
    column with such name and label (if specified) will be printed.

## [1.3.98]

### Fixed

- `otp.eval` used NestedOtq EP that lead to strange problems in case of using
  external query as a source for the `otp.Custom` symbols

## [1.3.97]

### Added

- `match_if_identical_times` argument to `join_by_time` method

## [1.3.96]

### Fixed

- `str.upper` and `str.lower` had `int` type instead of `str`
- `otp.Symbols` had no option to filter by the tick type, was introduced the `for_tick_type` parameter;
  introduced usability, added `pattern` and `show_tick_type` as parameters
- Fixed bug in the `.dump` method: a query with dump generated multiple outputs and `.to_df()` returned a random one
- Added ability to fetch sub-schema from the `schema` property on the Source
  using the list of columns, i.e. `data.schema[['QTY', 'PRICE']]`
- Fixed bug that `query_inspector` tried to parse a python file
  with `otq.query_creator` as a first stage query and generated QueryNotFoundError
- Access to non existing column in the schema led to creating a new column in the schema with the `float` type
- Deprecated `TTicks`
- Added the `strict` parameter to the `table` equal to the `keep_input_fields` flag in OT
- Added conversion from int to `nsectime`

## [1.3.95]

### Added

- Added the `high_time`, `low_time` aggregations (standalone box & compute)

## [1.3.94]

### Added

- Introduced the `dump` method

## [1.3.93]

### Added

- Validation that path to Onetick binary is in $PATH environment variable on Windows

## [1.3.92]

### Fixed

- *Dateparts* objects (`otp.Second`, `otp.Milli` and so on) are now correctly support
 initialization with difference between two lag operators

## [1.3.91]

### Fixed

- Timestamp field assignment with `_START_TIME` and `_END_TIME` is now possible

## [1.3.90]

### Added

- Support state variables assignment with `otp.datetime`

## [1.3.89]

### Added

- Support a forward lag operator for add and update (via temporary column) fields

## [1.3.88]

### Fixed

- derived databases are now saved after tests with `--keep-generated` flag

## [1.3.87]

### Added

- Support a column assignment with `otp.datetime`

## [1.3.86]

### Added

- schema for the symbol param source
- type conversion from `nsectime` to int
- presort support for merge with bound symbols

### Fixed

- ability to create a `otp.dt` from another `otp.dt` object
- update the `.Time` column based on operation

## [1.3.85]

### Added

- Support the TABLE EP as the `.table` method on the Source

### Removed

- Custom database like TAQ_NBBO are removed from the package, because they are custom

## [1.3.84]

### Added

- now it is possible to call methods (e.g. `fillna`, `round`, etc.) on expression, not only columns
- python way expressions for user filter, no need to compare with 0 or empty string\zero timestamp

### Fixed

- string + operation now doesn't fail with 3 or more operands
- accessors now works on state variables

## [1.3.83]

### Changed

- quotes in `eval` statements are now escaped with slashes, not `expr` as it was before

## [1.3.82]

### Added

- adaptive tick type documentation
- windows short path configuration description

## [1.3.81]

### Changed

- add otp.eval function for specifying symbol and start/end time of evaluated query or Source
- otp.query now supports expressions as parameter

### Fixed

- now it is possible to create timezone-aware `otp.datetime` object from datetime with no default timezone

## [1.3.80]

### Changed

- move onetick-py-test to 1.1.25 to grab cleaning unused file handlers

## [1.3.79]

### Changed

- move onetick-py-test to 1.1.24 version

## [1.3.78]

### Fixed

- passing `otp.sources.query` objects to the `Custom` as a symbol

## [1.3.77]

### Added

- add `pylama` static code checker to CI

## [1.3.76]

### Added

- `add_passthrough` flag to `Source.to_graph()` method

## [1.3.75]

### Added

- added supporting per-tick-script logic through the `.script` method on the `Source`
   supports only if-else, adding column and return statement
- introduced `BaseSchema` as property of the `_Source`
- introduced `Source` as alias for the `_Source`

## [1.3.74]

### Added

- `Ticks` and `Tick` can handle special `utils.adaptive` value in `tick_type` argument to use a tick type from the sync node.

## [1.3.73]

### Added

- `apply_query` and `otp.sources.query` now handle both queries with unbound and bound symbols (used to work only with bound)

## [1.3.72]

### Added

- `otp.datetime` now has nanoseconds precision
- `otp.Nano` and so on supports column and expression as the argument

## [1.3.71]

### Added

- Added ability to add external users into the generated acl files

## [1.3.70]

### Added

- `process_by_group` method to `Source` object

## [1.3.69]

### Fixed

- Changed type of Exception in case if query would not found

## [1.3.68]

### Fixed

- OMDSEQ field isn't renamed anymore by `join_by_time` function

## [1.3.67]

### Fixed

- Moved onetick-py-test version to get fix related to the using per-tick-script

## [1.3.66]

### Added

- properties `.db` and `.tick_type` to the `Custom` source

## [1.3.65]

### Added

- `isin` method for checking if column's value in items

## [1.3.64]

### Added

- ability to the join_by_time to set several leading sources

## [1.3.63]

### Fixed

- `otp.query` param substitution is taken quotes and slashes into account

## [1.3.62]

### Changed

- Speed up the Ticks() performance for simple cases

## [1.3.61]

### Changed

- Moved conftest.py logic into the onetick-py-test==1.1.21, and moved dependency forward

## [1.3.60]

### Fixed

- `OTQ_PATH` includes keep generated folder for sessions
- `tick_timestamp_type` db property is set to NANOS by default

## [1.3.59]

### Changed

- state variables are now collected in `.state_vars` field of the `_Source`

### Removed

- `otp.state.var` method

### Fixed

- stacking `otp.funcs.join` with `rprefix` specified doesn't cause an error anymore
- state variables weren't been copied after copy, merge and join operation

## [1.3.58]

### Added

- auto escaping in `otp.query` parameters
- ability to use onetick.py expression as `otp.query` parameters

## [1.3.57]

### Added

- `otp.inf` for double positive infinity

## [1.3.56]

### Added

- Numeric and boolean constants as indexes of source will cause ValueError

## [1.3.55]

### Added

- ability to pass tick-dependent expressions to `join_with_query` parameters `start_time` and `end_time`

## [1.3.54]

### Added

- `set_schema` method to specify schema in python part

## [1.3.53]

### Fixed

- `query_inspector` now works correctly with commented nodes and bound securities

## [1.3.52]

### Added

- `otp.Query` can now work on queries without unbound symbols without specifying `symbol=None`
- `otp.Query` now accepts an optional parameter `params`,
  which can contain a dictionary of parameters to be passed to the underlying query

### Fixed

- `query_inspector` now properly considers `symbol_param`-dependent `eval()` queries
  as needing a lower bound (or unbound) symbol

## [1.3.51]

### Fixed

- `GeneratedDir` might fail in the concurrent runs during the already existing directories

## [1.3.50]

### Fixed

- `otp.Custom` with symbol as `otp.query` now returns merged data, not dict

## [1.3.49]

### Added

- Add derived databases support in the `otp.DB`

## [1.3.48]

### Added

- Ability to add pins to a query

## [1.3.47]

### Changed

- Usability of debugging scripts

## [1.3.46]

### Changed

- Added **pid** to the base temp directory name to get rid of problems in multiprocessing environment

## [1.3.45]

### Changed

- Updated version of onetick.test

## [1.3.44]

### Fixed

- removed debug output

## [1.3.43]

### Fixed

- query_inspector now parses some old NESTED_OTQ formats properly

## [1.3.42]

### Added

- logic that allows to integrate debugging logic for temporary generated objects

## [1.3.41]

### Added

- query_inspector now can determine whether a query needs an unbound symbol list

## [1.3.40]

### Fixed

- fixed bug related to the defining types using the `__getitem__` method

## [1.3.39]

### Added

- specifying PYTHON_VERSION by default

## [1.3.38]

### Added

- caching and prefix parameters to `join_with_query`

## [1.3.37]

### Added

- Add symbols parameter to merge function to specify bound symbols for reading from db
- `otp.Custom` now accept `_Source` object and collections (but not dicts) as symbols.
- the `otp.Empty` source
- ability to pass many databases in the `Session.use` method

### Changed

- the `keep_db` flag set to False by default in the Symbols source
- the `join` option in the `join_with_query` method to how, according to other join operations

## [1.3.36]

### Added

- Implemented date difference in *datepart* units

## [1.3.35]

### Fixed

- Added ability to use the Custom source with API binding using the ONE_TICK_CONFIG environment variable

## [1.3.34]

### Fixed

- Time and TIMESTAMP columns type changed from the `msectime` to `nsectime` by default

## [1.3.33]

### Fixed

- the query_inspector did not work with DECLARE_STATE_VARIABLES nodes, that has a different format that other EPs

## [1.3.32]

### Added

- Check db name to be string

## [1.3.31]

### Added

- `join_with_query` method to `Source` object

### Removed

- `join_with_func` function

### Deprecated

- `symbol` method of Source object

## [1.3.30]

### Fixed

- Earlier the Symbols returned symbols with a database prefix,
  but we introduce a `keep_db` flag and default value broke backward-compatibility.
  Set default to True to keep compatibility

## [1.3.29]

### Fixed

- Nested queries short path doesn't work, when there is no `otp.Session`, but `ONE_TICK_CONFIG` is defined

## [1.3.28]

### Fixed

- Issue related to the hardcoded name of input and output pins for the nested queries

### Added

- `query_inspector` that reads a passed query and reveals a query structure

## [1.3.27]

### Added

- Support external unbound symbols syntax

### Fixed

- Bug of applying nested queries due the legacy `passthrough`

## [1.3.26]

### Added

- `otp.Year`, `otp.Quarter`, ..., `otp.Nano` classes can be used as parameter for + operation with timestamps

## [1.3.25]

### Changed

- set onetick-lib version to 1.0.3 (works only with version of OneTick >= update3_20190927120000)

## [1.3.24]

### Added

- regex support for `drop` method

## [1.3.23]

### Fixed

- join_by_time didn't assign name to a node in some conditions,
  that led to an error that one source didn't have name specified

## [1.3.22]

### Added

- check_schema flag to join_by_time for avoiding exception on joining Sources with columns's names changed by sink
  event processor

## [1.3.21]

### Fixed

- REGEX_EXTRACT supports case changing groups, while REGEX_REPLACE doesn't

## [1.3.20]

### Added

- automatically call `rtrim` on `replace` with column as a parameter

## [1.3.19]

### Added

- DISTINCT aggregation

## [1.3.18]

### Added

- `.float` accessor and `str` method to it

## [1.3.17]

### Deprecated

- `guess_schema` argument of `Custom` constructor is deprecated

### Added

- Custom constructor argument `schema_policy` (allowed values: manual, tolerant, fail)
  for better tuning of the schema guess behaviour

## [1.3.16]

### Added

- `str.substr` function

## [1.3.15]

### Added

- `str.extract` function

## [1.3.14]

### Added

- `str.repeat` function and string by non-negative int multiplication based on this function

## [1.3.13]

### Added

- `str.find` function

## [1.3.12]

### Changed

- Moved `locator_parser` forward to 1.0.4

## [1.3.11]

### Added

- `str.replace` and `str.regex_replace` functions

## [1.3.10]

### Added

- utils `omd_dist_path` that finds OneTick build path bases on PYTHONPATH

## [1.3.9]

### Added

- `str.lower` and `str.upper` functions

## [1.3.8]

### Added

- `str.trim`, `str.ltrim` and `str.rtrim` functions

## [1.3.7]

### Added

- columns generated by token saves parent dtype from now

## [1.3.6]

### Added

- ability to change config file for a session

## [1.3.5]

### Added

- `str.len` and `str.contains` functions

## [1.3.4]

### Added

- Random seed setup at the beginning from session

## [1.3.3]

### Fixed

- Logging in temporary files that creates a lot of captured logs with pytest,
  that happens when logging file destroyed before temporary objects destruction

## [1.3.2]

### Fixed

- Problem with overridden min/max functions: they missed `kwargs` params

## [1.3.1]

### Changed

- Update pytest dependency from 4.x to 5.x
- Update other pytest related dependencies

## [1.2.54]

### Added

- Parameter `shared_state_variables_list` to function `apply_query`

## [1.2.53]

### Fixed

- Fixed bug with query params which had not propagated by `apply_query` method
  in case `otp.sources.query.otp_query` used as query parameter

## [1.2.52]

### Fixed

- Partially defined DB locations. Also it starts to support optional location options like DAY_BOUNDARY_TZ

## [1.2.51]

### Fixed

- Add ability to pass pd.Timestamp values, because it is know about DST.
  Also fixed a bug that did not allow to set datetime with finer precision than seconds.

## [1.2.50]

### Fixed

- Added check for integration with onetick.query. Check raises a user friendly descriptive error message what to do.
  Also it cover a case when you had libraries for python with the standard malloc allocator in PYTHONPATH,
  but used python with the `py-malloc` allocator, then code rose magic problems on running queries
  but not on importing the onetick.query package.

## [1.2.49]

### Fixed

- `get_schema()` and the like now support derived databases properly

## [1.2.48]

### Fixed

- remove pytest-profiling

## [1.2.47]

### Added

- `get_schema()` set of utils; `guess_schema` parameter for Custom sources.

## [1.2.46]

### Fixed

- Remove ALERTS database from the dummy locator

## [1.2.45]

### Added

- Average aggregation; alternative (non-second) units for aggregations.

## [1.2.44]

### Fixed

- Redirected OneTick logs into a temporary log file

## [1.2.43]

### Fixed

- TmpFile does not take the confusing 'path' parameter

## [1.2.42]

### Added

- Short version of conditions in lambdas and functions, i.e. `<> if row.x else <>`

## [1.2.41]

### Added

- Support min and max functions

## [1.2.40]

### Added

- Partially support JWQ

## [1.2.39]

### Added

- Support the lag operation on a column

## [1.2.38]

### Fixed

- Remove quotes if they are passed when fill config file

## [1.2.36]

### Added

- Fixed support lambdas for sources

## [1.2.35]

### Added

- Added new accessor `match` which is equivalent to `regex_match` built-in function from OneTick.

## [1.2.34]

### Added

- Mod operation support.

## [1.2.33]

### Added

- `query` source config now supports dicts and lists as definitions of output columns.

## [1.2.32]

### Added

- Support custom and server based license

## [1.2.31]

### Fixed

- Assignment to timestamp field now applies a *pre-sort*.

## [1.2.30]

### Added

- Added `token` accessor for string which call "token" built-in function from OneTick.

## [1.2.29]

### Added

- Function `get_schema()` for getting the tick descriptors out of existing databases.

## [1.2.28]

### Fixed

- Restored public interface for write_to_db.

## [1.2.27]

### Added

- Support accessor for datetime and string

## [1.2.26]

### Fixed

- Sessions are allowed to use COMMAND_EXECUTE EP.

## [1.2.25]

### Fixed

- Fixed passing directory fixtures from the onetick.test packages to the Config constructor
  as `otq_path` and `csv_path` parameters without casting to string

## [1.2.24]

### Added

- Support for `join_by_time` **SAME_TIMESTAMP_JOIN_POLICY**.

## [1.2.23]

### Changed

- Interfaces for `join` and `join_by_time` functions.
- Added `merge` function that repeats the `concat` functions

## [1.2.22]

### Added

- Supported simple state variables

## [1.2.21]

### Fixed

- Tests for ticks taking dates as input now work on Windows.

## [1.2.20]

### Fixed

- Removing database from locator when some internal action was done (db.symbols, db.add ...)

## [1.2.19]

### Added

- Added property symbols to database object that returns all symbols in db.
- Added source with symbols to get object we can work with

## [1.2.18]

### Fixed

- Ability to add to db after session uses db

## [1.2.17]

### Added

- Tick and Ticks objects can now be generated with datetime values as parameters,
  which will be cast to OneTick nanosecond-precision timestamps.
  (Module datetime itself doesn't support nanosecond time, so the initial values can only have microsecond precision.)

## [1.2.16]

### Fixed

- CSV times are now read in proper (nanosecond) resolution.
- START/END timestamps are presented in proper (nanosecond) resolution.

## [1.2.15]

### Fixed

- `to_otq` query with `eval` in symbols

## [1.2.14]

### Added

- Added symbol params support

## [1.2.13]

### Added

- `use_local_license` flag to the `Config`; it is helpful when you don't want to use local license
  and want to use remote license from a tick server

## [1.2.12]

### Added

- Ability to add 'destroy' flag to acl for db

## [1.2.11]

### Added

- It's now possible to use pandas DataFrame's as a source for databases.

## [1.2.10]

### Added

- Ability to pass `eval` as an unbound symbol and `otp.source.query` as a symbol there

## [1.2.9]

### Fixed

- Fixed the `Ticks` for case when columns has non-None values, but with different types

## [1.2.8]

### Fixed

- `to_graph()` method to build the final graph instead of intermediate

## [1.2.7.1]

### Added

- `empty` flag to the locator, that allows to create a locator without stubs

## [1.2.7]

### Added

- Restriction to have multiple active sessions simultaneously

## [1.2.6]

### Added

- `.split` and `.switch` methods on the Source objects, that allows to split data. Cover SWITCH EP

## [1.2.5]

### Added

- Ability to include reference to a locator into a locator using `Locator().add(Locator('path/to/another.locator')`
- Default values for a location in the `DB` constructor.
- Allowed `WRITE_TEXT` in the default locator

## [1.2.4]

### Changed

- Predefined databases became singletons instead of instances. Example: before was `onetick.py.db.NYSE_TAQ()`,
  but changed to `onetick.py.db.NYSE_TAQ`.

## [1.2.3]

### Fixed

- Predefined db `NYSE_TAQ`

## [1.2.2]

### Added

- Ability to add and remove "Admin user" to an ACL
