module DB
  # Represents a statement to be executed in any of the connections
  # of the pool. The statement is not be executed in a prepared fashion.
  # The execution of the statement is retried according to the pool configuration.
  #
  # See `PoolStatement`
  class PoolPreparedStatement < PoolStatement
    def initialize(db : Database, query : String)
      super
    end

    protected def do_close
    end

    # builds a statement over a real connection
    # the connection is registered in `@connections`
    private def build_statement : Statement
      conn = @db.checkout
      begin
        stmt = conn.prepared.build(@query)
      rescue ex
        conn.release
        raise ex
      end
      stmt
    end
  end
end
