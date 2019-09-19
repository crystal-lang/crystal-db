module DB
  # When a statement is to be executed in a DB that has a connection pool
  # a statement from the DB needs to be able to represent a statement in any
  # of the connections of the pool. Otherwise the user will need to deal with
  # actual connections in some point.
  abstract class PoolStatement
    include StatementMethods

    def initialize(@db : Database, @query : String)
    end

    # See `QueryMethods#exec`
    def exec : ExecResult
      statement_with_retry &.exec
    end

    # See `QueryMethods#exec`
    def exec(*args_, args : Array? = nil) : ExecResult
      statement_with_retry &.exec(*args_, args: args)
    end

    # See `QueryMethods#query`
    def query : ResultSet
      statement_with_retry &.query
    end

    # See `QueryMethods#query`
    def query(*args_, args : Array? = nil) : ResultSet
      statement_with_retry &.query(*args_, args: args)
    end

    # See `QueryMethods#scalar`
    def scalar(*args_, args : Array? = nil)
      statement_with_retry &.scalar(*args_, args: args)
    end

    # builds a statement over a real connection
    # the conneciton is registered in `@connections`
    private abstract def build_statement : Statement

    private def statement_with_retry
      @db.retry do
        return yield build_statement
      end
    end
  end
end
