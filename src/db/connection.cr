module DB
  # Database driver implementors must subclass `Connection`.
  #
  # Represents one active connection to a database.
  #
  # Users should never instantiate a `Connection` manually. Use `DB#open` or `Database#connection`.
  #
  # Refer to `QueryMethods` for documentation about querying the database through this connection.
  #
  # ### Note to implementors
  #
  # The connection must be initialized in `#initialize` and closed in `#do_close`.
  #
  # Override `#build_prepared_statement` method in order to return a prepared `Statement` to allow querying.
  # Override `#build_unprepared_statement` method in order to return a unprepared `Statement` to allow querying.
  # See also `Statement` to define how the statements are executed.
  #
  # If at any give moment the connection is lost a DB::ConnectionLost should be raised. This will
  # allow the connection pool to try to reconnect or use another connection if available.
  #
  abstract class Connection
    include Disposable
    include SessionMethods(Connection, Statement)
    include BeginTransaction

    # :nodoc:
    getter database
    @statements_cache = StringKeyCache(Statement).new
    @transaction = false
    getter? prepared_statements : Bool

    def initialize(@database : Database)
      @prepared_statements = @database.prepared_statements?
    end

    # :nodoc:
    def fetch_or_build_prepared_statement(query)
      @statements_cache.fetch(query) { build_prepared_statement(query) }
    end

    # :nodoc:
    abstract def build_prepared_statement(query) : Statement

    # :nodoc:
    abstract def build_unprepared_statement(query) : Statement

    def begin_transaction
      raise DB::Error.new("There is an existing transaction in this connection") if @transaction
      @transaction = true
      create_transaction
    end

    protected def create_transaction : Transaction
      TopLevelTransaction.new(self)
    end

    protected def do_close
      @statements_cache.each_value &.close
      @statements_cache.clear
      @database.pool.delete self
    end

    # :nodoc:
    def release_from_statement
      @database.return_to_pool(self) unless @transaction
    end

    # :nodoc:
    def release_from_transaction
      @transaction = false
    end

    # :nodoc:
    def perform_begin_transaction
      self.unprepared.exec "BEGIN"
    end

    # :nodoc:
    def perform_commit_transaction
      self.unprepared.exec "COMMIT"
    end

    # :nodoc:
    def perform_rollback_transaction
      self.unprepared.exec "ROLLBACK"
    end

    # :nodoc:
    def perform_create_savepoint(name)
      self.unprepared.exec "SAVEPOINT #{name}"
    end

    # :nodoc:
    def perform_release_savepoint(name)
      self.unprepared.exec "RELEASE SAVEPOINT #{name}"
    end

    # :nodoc:
    def perform_rollback_savepoint(name)
      self.unprepared.exec "ROLLBACK TO #{name}"
    end
  end
end
