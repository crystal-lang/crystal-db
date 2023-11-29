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

    record Options,
      # Return whether the statements should be prepared by default
      prepared_statements : Bool = true,
      # Return whether the prepared statements should be cached or not
      prepared_statements_cache : Bool = true do
      def self.from_http_params(params : HTTP::Params, default = Options.new)
        Options.new(
          prepared_statements: DB.fetch_bool(params, "prepared_statements", default.prepared_statements),
          prepared_statements_cache: DB.fetch_bool(params, "prepared_statements_cache", default.prepared_statements)
        )
      end
    end

    # :nodoc:
    property context : ConnectionContext = SingleConnectionContext.default
    @statements_cache = StringKeyCache(Statement).new
    @transaction = false
    # :nodoc:
    property auto_release : Bool = true

    def initialize(@options : Options)
    end

    def prepared_statements? : Bool
      @options.prepared_statements
    end

    def prepared_statements_cache? : Bool
      @options.prepared_statements_cache
    end

    # :nodoc:
    def fetch_or_build_prepared_statement(query) : Statement
      if @options.prepared_statements_cache
        @statements_cache.fetch(query) { build_prepared_statement(query) }
      else
        build_prepared_statement(query)
      end
    end

    # :nodoc:
    abstract def build_prepared_statement(query) : Statement

    # :nodoc:
    abstract def build_unprepared_statement(query) : Statement

    def begin_transaction : Transaction
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
      context.discard self
    end

    # :nodoc:
    protected def before_checkout
      @auto_release = true
    end

    # :nodoc:
    protected def after_release
    end

    # return this connection to the pool
    # managed by the database. Should be used
    # only if the connection was obtained by `Database#checkout`.
    def release
      context.release(self)
    end

    # :nodoc:
    def release_from_statement
      self.release if @auto_release && !@transaction
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
