module DB
  # Database driver implementors must subclass `Driver`,
  # register with a driver_name using `DB#register_driver` and
  # override the factory method `#connection_builder`.
  #
  # ```
  # require "db"
  #
  # class FakeDriver < DB::Driver
  #   def connection_builder(uri : URI) : Proc(DB::Connection)
  #     params = HTTP::Params.parse(uri.query || "")
  #     options = connection_options(params)
  #     ->{ FakeConnection.new(options).as(DB::Connection) }
  #   end
  # end
  #
  # DB.register_driver "fake", FakeDriver
  # ```
  #
  # Access to this fake database will be available with
  #
  # ```
  # DB.open "fake://..." do |db|
  #   # ... use db ...
  # end
  # ```
  #
  # Refer to `Connection`, `Statement` and `ResultSet` for further
  # driver implementation instructions.
  #
  # Override `#connection_options` and `#pool_options` to provide custom
  # defaults or parsing of the connection string URI.
  abstract class Driver
    # Returns a new connection factory.
    #
    # NOTE: For implementors *uri* should be parsed once. If all the options
    # are sound a ConnectionBuilder is returned.
    abstract def connection_builder(uri : URI) : ConnectionBuilder

    def connection_options(params : HTTP::Params) : Connection::Options
      Connection::Options.from_http_params(params)
    end

    def pool_options(params : HTTP::Params) : Pool::Options
      Pool::Options.from_http_params(params)
    end
  end
end
