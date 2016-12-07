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
