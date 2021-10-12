module DB
  abstract class Connection
  end

  class Error < Exception
  end

  class MappingException < Error
    getter klass
    getter property

    def initialize(message, @klass : String, @property : String? = nil, cause : Exception? = nil)
      message = String.build do |io|
        io << message
        io << "\n  deserializing " << @klass
        if property = @property
          io << "#" << property
        end
      end
      super(message, cause: cause)
    end
  end

  class PoolTimeout < Error
  end

  class PoolRetryAttemptsExceeded < Error
  end

  class PoolResourceLost(T) < Error
    getter resource : T

    def initialize(@resource : T)
      @resource.close
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

  # Raised when the type returned for the column value
  # does not match the type expected.
  class ColumnTypeMismatchError < Error
    getter column_index : Int32
    getter column_name : String
    getter column_type : String
    getter expected_type : String

    def initialize(*, context : String, @column_index : Int32, @column_name : String, @column_type : String, @expected_type : String)
      super("In #{context} the column #{column_name} returned a #{column_type} but a #{expected_type} was expected.")
    end
  end
end
