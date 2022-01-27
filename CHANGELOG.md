## v0.11.0 (2022-01-27)

* Fix `Connection#transaction` method to return the block value as the result. ([#159](https://github.com/crystal-lang/crystal-db/pull/159), [#160](https://github.com/crystal-lang/crystal-db/pull/160), thanks @bcardiff)
* Add `DB::ColumnTypeMismatchError` error with column and type information. ([#156](https://github.com/crystal-lang/crystal-db/pull/156), thanks @jwoertink, @bcardiff)
* Improve `DB::MappingException` error. ([#129](https://github.com/crystal-lang/crystal-db/pull/129), thanks @straight-shoota)
* Close connection resource when connection is lost. ([#155](https://github.com/crystal-lang/crystal-db/pull/155), thanks @stakach, @bcardiff)
* Discard closed connections in the pool when they are returned. ([#154](https://github.com/crystal-lang/crystal-db/pull/154), thanks @stakach)
* Fix typo in `Mode.from_rs` argument type. ([#142](https://github.com/crystal-lang/crystal-db/pull/142), thanks @dukeraphaelng)
* Migrate CI to GitHub Actions. ([#147](https://github.com/crystal-lang/crystal-db/pull/147), [#152](https://github.com/crystal-lang/crystal-db/pull/152), thanks @oprypin, thanks @straight-shoota)

This release requires Crystal 1.0.0 or later.

Note: For drivers implementations [#156](https://github.com/crystal-lang/crystal-db/pull/156) adds a `abstract def next_column_index : Int32` to `ResultSet` so there is a breaking-change that does not affect consumers of the library.

## v0.10.1 (2021-03-22)

* Add docs for `DB::Database#setup_connection` ([#139](https://github.com/crystal-lang/crystal-db/pull/139), thanks @jgaskins)

## v0.10.0 (2020-09-30)

* Fix mutex deadlock in setup_connection. ([#128](https://github.com/crystal-lang/crystal-db/pull/128), thanks @straight-shoota)
* Add logging for executing queries. ([#134](https://github.com/crystal-lang/crystal-db/pull/134), thanks @bcardiff)
* Allow `DB::Pool` to be used a generic connection pool. ([#131](https://github.com/crystal-lang/crystal-db/pull/131), thanks @jgaskins)

This release requires Crystal 0.35.0 or later.

## v0.9.0 (2020-04-06)

* Fix compatibility issues for Crystal 0.34.0. ([#124](https://github.com/crystal-lang/crystal-db/pull/124), thanks @bcardiff)
* Fix readme sample. ([#117](https://github.com/crystal-lang/crystal-db/pull/117), thanks @bcardiff)
* Raise `DB::NoResultsError` when trying to get data from an empty result ([#121](https://github.com/crystal-lang/crystal-db/pull/121), [#125](https://github.com/crystal-lang/crystal-db/pull/125), thanks @jwoertink, @bcardiff)

## v0.8.0 (2019-12-11)

* Add `DB::Serializable`. ([#115](https://github.com/crystal-lang/crystal-db/pull/115), thanks @nickbclifford)

This release requires Crystal 0.25.0 or later.

## v0.7.0 (2019-09-20)

* **(breaking-change)** `#exec`, `#query`, `#scalar` methods require named argument for array values ([#110](https://github.com/crystal-lang/crystal-db/pull/110), thanks @straight-shoota)
* Fix pool issues when creating simultaneous connections. ([#109](https://github.com/crystal-lang/crystal-db/pull/109), thanks @bcardiff)
* Fix compatibility issues for upcoming Crystal 0.31.0. ([#111](https://github.com/crystal-lang/crystal-db/pull/111), thanks @bcardiff)
* Added `DB::Pool#stats`. ([#109](https://github.com/crystal-lang/crystal-db/pull/109), thanks @bcardiff)

## v0.6.0 (2019-08-02)

* Fix compatibility issues for Crystal 0.30.0. ([#108](https://github.com/crystal-lang/crystal-db/pull/108), thanks @bcardiff)
* Fix `BeginTransaction#transaction` rollback due to protocol error. ([#101](https://github.com/crystal-lang/crystal-db/pull/101), thanks @straight-shoota)
* CI includes Crystal nightly. ([#106](https://github.com/crystal-lang/crystal-db/pull/106), thanks @bcardiff)
* Add the Cassandra driver. ([#94](https://github.com/crystal-lang/crystal-db/pull/94), thanks @kaukas)
* Several docs improvements. ([#99](https://github.com/crystal-lang/crystal-db/pull/99), [#96](https://github.com/crystal-lang/crystal-db/pull/96), [#107](https://github.com/crystal-lang/crystal-db/pull/107), thanks @nickelghost, @greenbigfrog, @MatthiasWinkelmann)

## v0.5.1 (2018-11-07)

* Fix `QueryMethods#query_one?` handling no rows. ([#86](https://github.com/crystal-lang/crystal-db/pull/86), thanks @robdavid)
* Documentation improvements. ([#87](https://github.com/crystal-lang/crystal-db/pull/87), [#82](https://github.com/crystal-lang/crystal-db/pull/82), [#76](https://github.com/crystal-lang/crystal-db/pull/76), thanks @wontruefree, @Heaven31415, @vtambourine)

## v0.5.0 (2017-12-29)

* Fix compatibility issues for crystal 0.24.0. No changes in the api.

## v0.4.4 (2017-12-29)

* Allow query results to be read as named tuples directly (see [#56](https://github.com/crystal-lang/crystal-db/pull/56), thanks @Nephos)
* Fix sqlite samples in documentation (see [#71](https://github.com/crystal-lang/crystal-db/pull/71), thanks @hinrik)

## v0.4.3 (2017-11-07)

* Fix connections were not released when building invalid statements. (see [#65](https://github.com/crystal-lang/crystal-db/pull/65), thanks @crisward)
* Fix some exceptions were not deriving from `DB::Error`. (see [#70](https://github.com/crystal-lang/crystal-db/pull/70), thanks @exts)

## v0.4.2 (2017-04-21)

* Fix compatibility issues for crystal 0.22.0

## v0.4.1 (2017-04-10)

* Add spec helper for drivers. [#48](https://github.com/crystal-lang/crystal-db/pull/48)
* Add `#query_each`. [#18](https://github.com/crystal-lang/crystal-db/issues/18)
* Fix `#read(T.class)` to deal better with unhandled types.

## v0.4.0 (2017-03-20)

* Add `DB.connect` to create non pooled connections
* Add `Database#checkout` to allow explicit checkout/release connection (see #38)
* Fix `Mapping.from_rs` closes the result_set
* Fix `Mapping` works with nilable types (see #40, thanks @RX14)

## v0.3.3 (2016-12-24)

* Fix compatibility issues for crystal 0.20.3

## v0.3.2 (2016-12-16)

* Allow connection pool retry logic in `#scalar` queries.

## v0.3.1 (2016-12-15)

* Add ConnectionRefused exception to flag issues when opening new connections.

## v0.3.0 (2016-12-14)

* Add support for non prepared statements. [#25](https://github.com/crystal-lang/crystal-db/pull/25)

* Add support for transactions & nested transactions. [#27](https://github.com/crystal-lang/crystal-db/pull/27)

* Add `Bool` and `Time` to `DB::Any`.

## v0.2.2 (2016-12-06)

This release requires crystal 0.20.1

* Changed default connection pool size limit is now 0 (unlimited).

* Fixed allow new connections right away if pool can be increased.

## ~~v0.2.1 (2016-12-06)~~ [YANKED]

## v0.2.0 (2016-10-20)

* Fixed release DB connection if an exception occurs during execution of a query (thanks @ggiraldez)

## ~~v0.1.1 (2016-09-28)~~ [YANKED]

This release requires crystal 0.19.2

Note: v0.1.1 is yanked since is incompatible with v0.1.0 [more](https://github.com/crystal-lang/crystal-mysql/issues/10).

* Added connection pool. `DB.open` works with a underlying connection pool. Use `Database#using_connection` to ensure the same connection is been used across multiple statements. [more](https://github.com/crystal-lang/crystal-db/pull/12)

* Added mappings. JSON/YAML-like mapping macros (thanks @spalladino) [more](https://github.com/crystal-lang/crystal-db/pull/2)

* Changed require ResultSet implementors to just implement `read`, optionally implementing `read(T.class)`.

## v0.1.0 (2016-06-24)

* Initial release
