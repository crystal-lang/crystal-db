module DB
  module ConnectionContext
    # Return whether the statements should be prepared by default
    abstract def prepared_statements? : Bool

    # Indicates that the *connection* was permanently closed
    # and should not be used in the future.
    abstract def discard(connection : Connection)

    # Indicates that the *connection* is no longer needed
    # and can be reused in the future.
    abstract def release(connection : Connection)
  end

  # :nodoc:
  class SingleConnectionContext
    include ConnectionContext

    class_getter default : SingleConnectionContext = SingleConnectionContext.new(true)

    getter? prepared_statements : Bool

    def initialize(@prepared_statements : Bool)
    end

    def discard(connection : Connection)
    end

    def release(connection : Connection)
    end
  end
end
