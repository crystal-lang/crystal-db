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
  # Override `#build_prepared_statement` method in order to return a prepared `Statement` to allow querying.
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
    property? prepared_statements : Bool

    def initialize(@database : Database)
      @prepared_statements = @database.prepared_statements?
    end

    # :nodoc:
    def build(query) : Statement
      if prepared_statements?
        fetch_or_build_prepared_statement(query)
      else
        build_unprepared_statement(query)
      end
    end

    # :nodoc:
    def fetch_or_build_prepared_statement(query)
      @statements_cache.fetch(query) { build_prepared_statement(query) }
    end

    abstract def build_prepared_statement(query) : Statement

    abstract def build_unprepared_statement(query) : Statement

    protected def do_close
      @statements_cache.each_value &.close
      @statements_cache.clear
      @database.pool.delete self
    end

    # dsl helper to build prepared statements
    # returns a value that includes `QueryMethods`
    def prepared
      PreparedQuery.new(self)
    end

    # Returns a prepared `Statement` that has not been executed yet.
    def prepared(query)
      prepared.build(query)
    end

    # dsl helper to build unprepared statements
    # returns a value that includes `QueryMethods`
    def unprepared
      UnpreparedQuery.new(self)
    end

    # Returns an unprepared `Statement` that has not been executed yet.
    def unprepared(query)
      unprepared.build(query)
    end

    struct PreparedQuery
      include QueryMethods

      def initialize(@connection : Connection)
      end

      def build(query)
        @connection.fetch_or_build_prepared_statement(query)
      end
    end

    struct UnpreparedQuery
      include QueryMethods

      def initialize(@connection : Connection)
      end

      def build(query)
        @connection.build_unprepared_statement(query)
      end
    end
  end
end
