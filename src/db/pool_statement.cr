module DB
  # When a statement is to be executed in a DB that has a connection pool
  # a statement from the DB needs to be able to represent a statement in any
  # of the connections of the pool. Otherwise the user will need to deal with
  # actual connections in some point.
  class PoolStatement
    include StatementMethods

    @statements = {} of Connection => Statement

    def initialize(@db : Database, @query : String)
      # Prepares a statement on some connection
      # otherwise the preparation is delayed until the first execution.
      # After the first initialization the connection must be released
      # it will be checked out when executing it.
      get_statement.release_connection
      # TODO use a round-robin selection in the pool so multiple sequentially
      #      initialized statements are assigned to different connections.
    end

    protected def do_close
      # TODO close all statements on all connections.
      # currently statements are closed when the connection is closed.

      # WHAT-IF the connection is busy? Should each statement be able to
      # deallocate itself when the connection is free.
      @statements.clear
    end

    # See `QueryMethods#exec`
    def exec : ExecResult
      get_statement.exec
    end

    # See `QueryMethods#exec`
    def exec(*args) : ExecResult
      get_statement.exec(*args)
    end

    # See `QueryMethods#exec`
    def exec(args : Array) : ExecResult
      get_statement.exec(args)
    end

    # See `QueryMethods#query`
    def query : ResultSet
      get_statement.query
    end

    # See `QueryMethods#query`
    def query(*args) : ResultSet
      get_statement.query(*args)
    end

    # See `QueryMethods#query`
    def query(args : Array) : ResultSet
      get_statement.query(args)
    end

    # builds a statement over a real connection
    # the conneciton and the stament is registered in `@statements`
    private def get_statement : Statement
      conn, existing = @db.checkout_some(@statements.keys)
      if existing
        @statements[conn]
      else
        stmt = conn.prepare @query
        @statements[conn] = stmt
        stmt
      end
    end
  end
end
