require "http/params"
require "weak_ref"

module DB
  # Acts as an entry point for database access.
  # Connections are managed by a pool.
  # Use `DB#open` to create a `Database` instance.
  #
  # Refer to `QueryMethods` and `SessionMethods` for documentation about querying the database.
  #
  # ## Database URI
  #
  # Connection parameters are usually in a URI. The format is specified by the individual
  # database drivers, yet there are some common properties names usually shared.
  # See the [reference book](https://crystal-lang.org/reference/database/) for examples.
  #
  # The connection pool can be configured from URI parameters:
  #
  #   - `initial_pool_size` (default 1)
  #   - `max_pool_size` (default 0 = unlimited)
  #   - `max_idle_pool_size` (default 1)
  #   - `checkout_timeout` (default 5.0)
  #   - `retry_attempts` (default 1)
  #   - `retry_delay` (in seconds, default 1.0)
  #
  # When querying a database, prepared statements are used by default.
  # This can be changed from the `prepared_statements` URI parameter:
  #
  #   - `prepared_statements` (true, or false, default true)
  #
  class Database
    include SessionMethods(Database, PoolStatement)
    include ConnectionContext

    # :nodoc:
    getter pool

    @connection_options : Connection::Options
    @pool : Pool(Connection)
    @setup_connection : Connection -> Nil

    # Initialize a database with the specified options and connection factory.
    # This covers more advanced use cases that might not be supported by an URI connection string such as tunneling connection.
    def initialize(connection_options : Connection::Options, pool_options : Pool::Options, &factory : -> Connection)
      @connection_options = connection_options
      @setup_connection = ->(conn : Connection) { }
      @pool = uninitialized Pool(Connection) # in order to use self in the factory proc
      @pool = Pool(Connection).new(pool_options) {
        conn = factory.call
        conn.auto_release = false
        conn.context = self
        @setup_connection.call conn
        conn
      }
    end

    def prepared_statements? : Bool
      @connection_options.prepared_statements
    end

    def prepared_statements_cache? : Bool
      @connection_options.prepared_statements_cache
    end

    # Run the specified block every time a new connection is established, yielding the new connection
    # to the block.
    #
    # ```
    # db = DB.open(DB_URL)
    # db.setup_connection do |connection|
    #   connection.exec "SET TIME ZONE 'America/New_York'"
    # end
    # ```
    def setup_connection(&proc : Connection -> Nil)
      @setup_connection = proc
      @pool.each_resource do |conn|
        @setup_connection.call conn
      end
    end

    # Closes all connection to the database.
    def close
      @pool.close
    end

    # :nodoc:
    def discard(connection : Connection)
      @pool.delete connection
    end

    # :nodoc:
    def release(connection : Connection)
      @pool.release connection
    end

    # :nodoc:
    def fetch_or_build_prepared_statement(query) : PoolStatement
      PoolPreparedStatement.new(self, query)
    end

    # :nodoc:
    def build_unprepared_statement(query) : PoolStatement
      PoolUnpreparedStatement.new(self, query)
    end

    # :nodoc:
    def checkout_some(candidates : Enumerable(WeakRef(Connection))) : {Connection, Bool}
      @pool.checkout_some candidates
    end

    # yields a connection from the pool
    # the connection is returned to the pool
    # when the block ends
    def using_connection(&)
      connection = self.checkout
      begin
        yield connection
      ensure
        connection.release
      end
    end

    # returns a connection from the pool
    # the returned connection must be returned
    # to the pool by explictly calling `Connection#release`
    def checkout
      connection = @pool.checkout
      connection.auto_release = false
      connection
    end

    # yields a `Transaction` from a connection of the pool
    # Refer to `BeginTransaction#transaction` for documentation.
    def transaction(&)
      using_connection do |cnn|
        cnn.transaction do |tx|
          yield tx
        end
      end
    end

    # :nodoc:
    def retry(&)
      @pool.retry do
        yield
      end
    end
  end
end
