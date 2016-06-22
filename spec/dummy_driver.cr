require "spec"
require "../src/db"

class DummyDriver < DB::Driver
  def build_connection(db : DB::Database) : DB::Connection
    DummyConnection.new(db)
  end

  class DummyConnection < DB::Connection
    def initialize(db)
      super(db)
      @@connections ||= [] of DummyConnection
      @@connections.not_nil! << self
    end

    def self.connections
      @@connections.not_nil!
    end

    def self.clear_connections
      @@connections.try &.clear
    end

    def build_statement(query)
      DummyStatement.new(self, query)
    end

    def last_insert_id : Int64
      0
    end

    protected def do_close
      super
    end
  end

  class DummyStatement < DB::Statement
    property params

    def initialize(connection, @query : String)
      @params = Hash(Int32 | String, DB::Any).new
      super(connection)
    end

    protected def perform_query(args : Enumerable)
      set_params args
      DummyResultSet.new self, @query
    end

    protected def perform_exec(args : Enumerable)
      set_params args
      DB::ExecResult.new 0i64, 0_i64
    end

    private def set_params(args)
      @params.clear
      args.each_with_index do |arg, index|
        set_param(index, arg)
      end
    end

    private def set_param(index, value : DB::Any)
      @params[index] = value
    end

    private def set_param(index, value)
      raise "not implemented for #{value.class}"
    end

    protected def do_close
      super
    end
  end

  class DummyResultSet < DB::ResultSet
    @@next_column_type = String
    @top_values : Array(Array(String))
    @values : Array(String)?

    @@last_result_set : self?
    @@next_column_type : Nil.class | String.class | Int32.class | Int64.class | Float32.class | Float64.class | Slice(UInt8).class

    def initialize(statement, query)
      super(statement)
      @top_values = query.split.map { |r| r.split(',') }.to_a

      @@last_result_set = self
    end

    protected def do_close
      super
    end

    def self.last_result_set
      @@last_result_set.not_nil!
    end

    def move_next
      @values = @top_values.shift?
      !!@values
    end

    def column_count
      2
    end

    def column_name(index)
      "c#{index}"
    end

    def column_type(index : Int32)
      @@next_column_type
    end

    def self.next_column_type=(value)
      @@next_column_type = value
    end

    private def read? : DB::Any?
      n = @values.not_nil!.shift?
      raise "end of row" if n.is_a?(Nil)
      return nil if n == "NULL"

      if n == "?"
        return (@statement.as(DummyStatement)).params[0]
      end

      return n
    end

    def read?(t : Nil.class)
      read?.as(Nil)
    end

    def read?(t : String.class)
      read?.try &.to_s
    end

    def read?(t : Int32.class)
      read?(String).try &.to_i32
    end

    def read?(t : Int64.class)
      read?(String).try &.to_i64
    end

    def read?(t : Float32.class)
      read?(String).try &.to_f32
    end

    def read?(t : Float64.class)
      read?(String).try &.to_f64
    end

    def read?(t : Slice(UInt8).class)
      value = read?
      if value.is_a?(Nil)
        value
      elsif value.is_a?(String)
        ary = value.bytes
        Slice.new(ary.to_unsafe, ary.size)
      elsif value.is_a?(Slice(UInt8))
        value
      else
        raise "#{value} is not convertible to Slice(UInt8)"
      end
    end
  end
end

DB.register_driver "dummy", DummyDriver

class Witness
  getter count

  def initialize(@count = 1)
  end

  def check
    @count -= 1
  end
end

def with_witness(count = 1)
  w = Witness.new(count)
  yield w
  w.count.should eq(0), "The expected coverage was unmet"
end

def with_dummy
  DummyDriver::DummyConnection.clear_connections

  DB.open "dummy://host" do |db|
    yield db
  end
end
