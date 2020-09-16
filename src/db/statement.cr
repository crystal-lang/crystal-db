module DB
  # Common interface for connection based statements
  # and for connection pool statements.
  module StatementMethods
    include Disposable

    protected def do_close
    end

    # See `QueryMethods#scalar`
    def scalar(*args_, args : Array? = nil)
      query(*args_, args: args) do |rs|
        rs.each do
          return rs.read
        end
      end

      raise NoResultsError.new("no results")
    end

    # See `QueryMethods#query`
    def query(*args_, args : Array? = nil)
      rs = query(*args_, args: args)
      yield rs ensure rs.close
    end

    # See `QueryMethods#exec`
    abstract def exec : ExecResult
    # See `QueryMethods#exec`
    abstract def exec(*args_, args : Array? = nil) : ExecResult

    # See `QueryMethods#query`
    abstract def query : ResultSet
    # See `QueryMethods#query`
    abstract def query(*args_, args : Array? = nil) : ResultSet
  end

  # Represents a query in a `Connection`.
  # It should be created by `QueryMethods`.
  #
  # ### Note to implementors
  #
  # 1. Subclass `Statements`
  # 2. `Statements` are created from a custom driver `Connection#prepare` method.
  # 3. `#perform_query` executes a query that is expected to return a `ResultSet`
  # 4. `#perform_exec` executes a query that is expected to return an `ExecResult`
  # 6. `#do_close` is called to release the statement resources.
  abstract class Statement
    include StatementMethods

    # :nodoc:
    getter connection

    getter command : String

    def initialize(@connection : Connection, @command : String)
    end

    def release_connection
      @connection.release_from_statement
    end

    # See `QueryMethods#exec`
    def exec : DB::ExecResult
      perform_exec_and_release(Slice(Any).empty)
    end

    # See `QueryMethods#exec`
    def exec(*args_, args : Array? = nil) : DB::ExecResult
      perform_exec_and_release(EnumerableConcat.build(args_, args))
    end

    # See `QueryMethods#query`
    def query : DB::ResultSet
      perform_query_with_rescue Tuple.new
    end

    # See `QueryMethods#query`
    def query(*args_, args : Array? = nil) : DB::ResultSet
      perform_query_with_rescue(EnumerableConcat.build(args_, args))
    end

    private def perform_exec_and_release(args : Enumerable) : ExecResult
      before_query_or_exec(args)
      return perform_exec(args)
    ensure
      after_query_or_exec(args)
      release_connection
    end

    private def perform_query_with_rescue(args : Enumerable) : ResultSet
      before_query_or_exec(args)
      return perform_query(args)
    rescue e : Exception
      # Release connection only when an exception occurs during the query
      # execution since we need the connection open while the ResultSet is open
      release_connection
      raise e
    ensure
      after_query_or_exec(args)
    end

    protected abstract def perform_query(args : Enumerable) : ResultSet
    protected abstract def perform_exec(args : Enumerable) : ExecResult

    protected def before_query_or_exec(args : Enumerable)
      emit_log(args)
    end

    protected def after_query_or_exec(args : Enumerable)
    end

    protected def emit_log(args : Enumerable)
      Log.debug &.emit("Executing query", query: command, args: MetadataValueConverter.arg_to_log(args))
    end
  end

  # This module converts DB supported values to `::Log::Metadata::Value`
  #
  # ### Note to implementors
  #
  # If the driver defines custom types to be used as arguments the default behavior
  # will be converting the value via `#to_s`. Otherwise you can define overloads to
  # change this behaviour.
  #
  # ```
  # module DB::MetadataValueConverter
  #   def self.arg_to_log(arg : PG::Geo::Point)
  #     ::Log::Metadata::Value.new("(#{arg.x}, #{arg.y})::point")
  #   end
  # end
  # ```
  module MetadataValueConverter
    # Returns *arg* encoded as a `::Log::Metadata::Value`.
    def self.arg_to_log(arg) : ::Log::Metadata::Value
      ::Log::Metadata::Value.new(arg.to_s)
    end

    # :ditto:
    def self.arg_to_log(arg : Enumerable) : ::Log::Metadata::Value
      ::Log::Metadata::Value.new(arg.to_a.map { |a| arg_to_log(a).as(::Log::Metadata::Value) })
    end

    # :ditto:
    def self.arg_to_log(arg : Int) : ::Log::Metadata::Value
      ::Log::Metadata::Value.new(arg.to_i64)
    end

    # :ditto:
    def self.arg_to_log(arg : UInt64) : ::Log::Metadata::Value
      ::Log::Metadata::Value.new(arg.to_s)
    end

    # :ditto:
    def self.arg_to_log(arg : Nil | Bool | Int32 | Int64 | Float32 | Float64 | String | Time) : ::Log::Metadata::Value
      ::Log::Metadata::Value.new(arg)
    end
  end
end
