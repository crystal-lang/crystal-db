module DB
  # Acts as an entry point for database access.
  # Currently it creates a single connection to the database.
  # Eventually a connection pool will be handled.
  #
  # It should be created from DB module. See `DB#open`.
  #
  # Refer to `QueryMethods` for documentation about querying the database.
  class Database
    # :nodoc:
    getter driver

    # Returns the uri with the connection settings to the database
    getter uri

    @connection : Connection?

    # :nodoc:
    def initialize(@driver : Driver, @uri : URI)
      @in_pool = true
      @connection = @driver.build_connection(self)
    end

    # Closes all connection to the database.
    def close
      @connection.try &.close
      # prevent GC Warning: Finalization cycle involving discovered by mysql implementation
      @connection = nil
    end

    # :nodoc:
    def prepare(query)
      get_from_pool.prepare(query)
    end

    # :nodoc:
    def get_from_pool
      raise "DB Pool Exhausted" unless @in_pool
      @in_pool = false
      @connection.not_nil!
    end

    # :nodoc:
    def return_to_pool(connection)
      @in_pool = true
    end

    # yields a connection from the pool
    # the connection is returned to the pool after
    # when the block ends
    def using_connection
      connection = get_from_pool
      yield connection
    ensure
      return_to_pool connection
    end

    include QueryMethods
  end
end
