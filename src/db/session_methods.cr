module DB
  # Methods that are shared accross session like objects:
  #   - Database
  #   - Connection
  #
  # Classes that includes this module are able to execute
  # queries and statements in both prepared and unprepared fashion.
  #
  # This module serves for dsl reuse over session like objects.
  module SessionMethods(Session, Stmt)
    include QueryMethods(Stmt)

    # Returns whether by default the statements should
    # be prepared or not.
    abstract def prepared_statements? : Bool

    abstract def prepared_statements_cache? : Bool

    abstract def fetch_or_build_prepared_statement(query) : Stmt

    abstract def build_unprepared_statement(query) : Stmt

    def build(query) : Stmt
      if prepared_statements?
        stmt = fetch_or_build_prepared_statement(query)

        # #build is a :nodoc: method used on QueryMethods where
        # the statements are not exposed. As such if the cache
        # is disabled we should auto_close the statement.
        # When the statements are build explicitly the #prepared
        # and #unprepared methods are used. In that case the
        # statement is closed by the user explicitly also.
        if !prepared_statements_cache?
          stmt.auto_close = true if stmt.responds_to?(:auto_close=)
        end

        stmt
      else
        build_unprepared_statement(query)
      end
    end

    # dsl helper to build prepared statements
    # returns a value that includes `QueryMethods`
    def prepared
      PreparedQuery(Session, Stmt).new(self)
    end

    # Returns a prepared `Statement` that has not been executed yet.
    def prepared(query)
      prepared.build(query)
    end

    # dsl helper to build unprepared statements
    # returns a value that includes `QueryMethods`
    def unprepared
      UnpreparedQuery(Session, Stmt).new(self)
    end

    # Returns an unprepared `Statement` that has not been executed yet.
    def unprepared(query)
      unprepared.build(query)
    end

    struct PreparedQuery(Session, Stmt)
      include QueryMethods(Stmt)

      def initialize(@session : Session)
      end

      def build(query) : Stmt
        @session.fetch_or_build_prepared_statement(query)
      end
    end

    struct UnpreparedQuery(Session, Stmt)
      include QueryMethods(Stmt)

      def initialize(@session : Session)
      end

      def build(query) : Stmt
        @session.build_unprepared_statement(query)
      end
    end
  end
end
