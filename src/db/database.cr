require "http/params"

module DB
  # Acts as an entry point for database access.
  # Connections are managed by a pool.
  # The connection pool can be configured from URI parameters:
  #
  #   - initial_pool_size (default 1)
  #   - max_pool_size (default 1)
  #   - max_idle_pool_size (default 1)
  #   - checkout_timeout (default 5.0)
  #
  # It should be created from DB module. See `DB#open`.
  #
  # Refer to `QueryMethods` for documentation about querying the database.
  class Database
    # :nodoc:
    getter driver

    # Returns the uri with the connection settings to the database
    getter uri

    @pool : Pool(Connection)

    # :nodoc:
    def initialize(@driver : Driver, @uri : URI)
      # TODO: PR HTTP::Params.new -> HTTP::Params.new(Hash(String, Array(String)).new)
      params = (query = uri.query) ? HTTP::Params.parse(query) : HTTP::Params.new(Hash(String, Array(String)).new)
      pool_options = @driver.connection_pool_options(params)

      @pool = uninitialized Pool(Connection) # in order to use self in the factory proc
      @pool = Pool.new(->{ @driver.build_connection(self).as(Connection) }, **pool_options)
    end

    # Closes all connection to the database.
    def close
      @pool.close
    end

    # :nodoc:
    def prepare(query)
      conn = get_from_pool
      begin
        conn.prepare(query)
      rescue ex
        return_to_pool(conn)
        raise ex
      end
    end

    # :nodoc:
    def get_from_pool
      @pool.checkout
    end

    # :nodoc:
    def return_to_pool(connection)
      @pool.release connection
    end

    # yields a connection from the pool
    # the connection is returned to the pool after
    # when the block ends
    def using_connection
      connection = get_from_pool
      begin
        yield connection
      ensure
        return_to_pool connection
      end
    end

    include QueryMethods
  end
end
