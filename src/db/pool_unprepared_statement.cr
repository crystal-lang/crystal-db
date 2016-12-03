module DB
  # Represents a statement to be executed in any of the connections
  # of the pool. The statement is not be executed in a non prepared fashion.
  # The execution of the statement is retried according to the pool configuration.
  #
  # See `PoolStatement`
  class PoolUnpreparedStatement
    include StatementMethods

    def initialize(@db : Database, @query : String)
    end

    protected def do_close
      # unprepared statements do not need to be release in each connection
    end

    # See `QueryMethods#exec`
    def exec : ExecResult
      statement_with_retry &.exec
    end

    # See `QueryMethods#exec`
    def exec(*args) : ExecResult
      statement_with_retry &.exec(*args)
    end

    # See `QueryMethods#exec`
    def exec(args : Array) : ExecResult
      statement_with_retry &.exec(args)
    end

    # See `QueryMethods#query`
    def query : ResultSet
      statement_with_retry &.query
    end

    # See `QueryMethods#query`
    def query(*args) : ResultSet
      statement_with_retry &.query(*args)
    end

    # See `QueryMethods#query`
    def query(args : Array) : ResultSet
      statement_with_retry &.query(args)
    end

    # builds a statement over a real connection
    private def build_statement
      @db.pool.checkout.unprepared.build(@query)
    end

    private def statement_with_retry
      @db.retry do
        return yield build_statement
      end
    end
  end
end
