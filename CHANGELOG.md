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
