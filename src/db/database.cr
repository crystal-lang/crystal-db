require "http/params"
require "weak_ref"

module DB
  # Acts as an entry point for database access.
  # Connections are managed by a pool.
  # The connection pool can be configured from URI parameters:
  #
  #   - initial_pool_size (default 1)
  #   - max_pool_size (default 0 = unlimited)
  #   - max_idle_pool_size (default 1)
  #   - checkout_timeout (default 5.0)
  #   - retry_attempts (default 1)
  #   - retry_delay (in seconds, default 1.0)
  #
  # It should be created from DB module. See `DB#open`.
  #
  # Refer to `QueryMethods` for documentation about querying the database.
  class Database
    include QueryMethods

    # :nodoc:
    getter driver
    # :nodoc:
    getter pool

    # Returns the uri with the connection settings to the database
    getter uri

    getter? prepared_statements : Bool

    @pool : Pool(Connection)
    @setup_connection : Connection -> Nil
    @statements_cache = StringKeyCache(PoolPreparedStatement).new

    # :nodoc:
    def initialize(@driver : Driver, @uri : URI)
      params = HTTP::Params.parse(uri.query || "")
      @prepared_statements = DB.fetch_bool(params, "prepared_statements", true)
      pool_options = @driver.connection_pool_options(params)

      @setup_connection = ->(conn : Connection) {}
      @pool = uninitialized Pool(Connection) # in order to use self in the factory proc
      @pool = Pool.new(**pool_options) {
        conn = @driver.build_connection(self).as(Connection)
        @setup_connection.call conn
        conn
      }
    end

    def setup_connection(&proc : Connection -> Nil)
      @setup_connection = proc
      @pool.each_resource do |conn|
        @setup_connection.call conn
      end
    end

    # Closes all connection to the database.
    def close
      @statements_cache.each_value &.close
      @statements_cache.clear

      @pool.close
    end

    # :nodoc:
    def build(query) : PoolStatement
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

    # :nodoc:
    def build_prepared_statement(query)
      PoolPreparedStatement.new(self, query)
    end

    # :nodoc:
    def build_unprepared_statement(query)
      PoolUnpreparedStatement.new(self, query)
    end

    # :nodoc:
    def checkout_some(candidates : Enumerable(WeakRef(Connection))) : {Connection, Bool}
      @pool.checkout_some candidates
    end

    # :nodoc:
    def return_to_pool(connection)
      @pool.release connection
    end

    # yields a connection from the pool
    # the connection is returned to the pool after
    # when the block ends
    def using_connection
      connection = @pool.checkout
      begin
        yield connection
      ensure
        return_to_pool connection
      end
    end

    # :nodoc:
    def retry
      @pool.retry do
        yield
      end
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

      def initialize(@db : Database)
      end

      def build(query)
        @db.fetch_or_build_prepared_statement(query)
      end
    end

    struct UnpreparedQuery
      include QueryMethods

      def initialize(@db : Database)
      end

      def build(query)
        @db.build_unprepared_statement(query)
      end
    end
  end
end
