module DB
  # Database driver implementors must subclass `Driver`,
  # register with a driver_name using `DB#register_driver` and
  # override the factory method `#build_connection`.
  #
  # ```
  # require "db"
  #
  # class FakeDriver < DB::Driver
  #   def build_connection(context : DB::ConnectionContext)
  #     FakeConnection.new context
  #   end
  # end
  #
  # DB.register_driver "fake", FakeDriver
  # ```
  #
  # Access to this fake datbase will be available with
  #
  # ```
  # DB.open "fake://..." do |db|
  #   # ... use db ...
  # end
  # ```
  #
  # Refer to `Connection`, `Statement` and `ResultSet` for further
  # driver implementation instructions.
  abstract class Driver
    # Returns a new connection factory.
    #
    # NOTE: For implementors *uri* should be parsed once. If all the options
    # are sound a factory Proc is returned.
    abstract def connection_builder(uri : URI) : Proc(Connection)

    def connection_options(params : HTTP::Params) : Connection::Options
      Connection::Options.from_http_params(params)
    end

    def pool_options(params : HTTP::Params) : Pool::Options
      Pool::Options.from_http_params(params)
    end
  end
end
