module DB
  class Error < Exception
  end

  class MappingException < Exception
  end

  class PoolTimeout < Error
  end

  class PoolRetryAttemptsExceeded < Error
  end

  class ConnectionLost < Error
    getter connection : Connection

    def initialize(@connection)
    end
  end
end
