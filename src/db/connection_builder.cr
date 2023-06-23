module DB
  # A connection factory with a specific configuration.
  #
  # See `Driver#connection_builder`.
  abstract class ConnectionBuilder
    abstract def build : Connection
  end
end
