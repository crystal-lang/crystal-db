module DB
  # Database driver implementors must subclass `Connection`.
  #
  # Represents one active connection to a database.
  #
  # Users should never instantiate a `Connection` manually. Use `DB#open` or `Database#connection`.
  #
  # Refer to `QueryMethods` for documentation about querying the database through this connection.
  #
  # ### Note to implementors
  #
  # The connection must be initialized in `#initialize` and closed in `#do_close`.
  #
  # Override `#build_statement` method in order to return a prepared `Statement` to allow querying.
  # See also `Statement` to define how the statements are executed.
  #
  # If at any give moment the connection is lost a DB::ConnectionLost should be raised. This will
  # allow the connection pool to try to reconnect or use another connection if available.
  #
  abstract class Connection
    include Disposable
    include QueryMethods

    # :nodoc:
    getter database
    @statements_cache = StringKeyCache(Statement).new

    def initialize(@database : Database)
    end

    # :nodoc:
    def prepare(query) : Statement
      @statements_cache.fetch(query) { build_statement(query) }
    end

    abstract def build_statement(query) : Statement

    protected def do_close
      @statements_cache.each_value &.close
      @statements_cache.clear
    end
  end
end
