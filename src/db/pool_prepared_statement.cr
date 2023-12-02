module DB
  # Represents a statement to be executed in any of the connections
  # of the pool. The statement is not be executed in a prepared fashion.
  # The execution of the statement is retried according to the pool configuration.
  #
  # See `PoolStatement`
  struct PoolPreparedStatement < PoolStatement
    def initialize(db : Database, query : String)
      super
    end

    # builds a statement over a real connection
    private def build_statement : Statement
      conn = @db.pool.checkout
      begin
        conn.prepared.build(@query)
      rescue ex
        conn.release
        raise ex
      end
    end
  end
end
