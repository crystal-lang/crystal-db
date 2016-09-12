module DB
  # When a statement is to be executed in a DB that has a connection pool
  # a statement from the DB needs to be able to represent a statement in any
  # of the connections of the pool. Otherwise the user will need to deal with
  # actual connections in some point.
  class PoolStatement
    include StatementMethods

    # connections where the statement was prepared
    @connections = Set(WeakRef(Connection)).new

    def initialize(@db : Database, @query : String)
      # Prepares a statement on some connection
      # otherwise the preparation is delayed until the first execution.
      # After the first initialization the connection must be released
      # it will be checked out when executing it.
      statement_with_retry &.release_connection
      # TODO use a round-robin selection in the pool so multiple sequentially
      #      initialized statements are assigned to different connections.
    end

    protected def do_close
      # TODO close all statements on all connections.
      # currently statements are closed when the connection is closed.

      # WHAT-IF the connection is busy? Should each statement be able to
      # deallocate itself when the connection is free.
      @connections.clear
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
    # the conneciton is registered in `@connections`
    private def build_statement
      clean_connections
      conn, existing = @db.checkout_some(@connections)
      @connections << WeakRef.new(conn) unless existing
      conn.prepare(@query)
    end

    private def clean_connections
      # remove disposed or closed connections
      @connections.each do |ref|
        conn = ref.target
        if !conn || conn.closed?
          @connections.delete ref
        end
      end
    end

    private def statement_with_retry
      @db.retry do
        return yield build_statement
      end
    end
  end
end
