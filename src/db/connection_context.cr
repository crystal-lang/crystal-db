module DB
  module ConnectionContext
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

    class_getter default : SingleConnectionContext = SingleConnectionContext.new

    def discard(connection : Connection)
    end

    def release(connection : Connection)
    end
  end
end
