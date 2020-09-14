module DB
  abstract class Connection
  end

  class Error < Exception
  end

  class MappingException < Error
  end

  class PoolTimeout < Error
  end

  class PoolRetryAttemptsExceeded < Error
  end

  class PoolResourceLost(T) < Error
    getter resource : T

    def initialize(@resource : T)
    end
  end

  class PoolResourceRefused < Error
  end

  # Raised when an established connection is lost
  # probably due to socket/network issues.
  # It is used by the connection pool retry logic.
  class ConnectionLost < PoolResourceLost(Connection)
    def connection
      resource
    end
  end

  # Raised when a connection is unable to be established
  # probably due to socket/network or configuration issues.
  # It is used by the connection pool retry logic.
  class ConnectionRefused < PoolResourceRefused
  end

  class Rollback < Error
  end

  # Raised when a scalar query returns no results.
  class NoResultsError < Error
  end
end
