require "./spec_helper"

class GenericResultSet(T) < DB::ResultSet
  def initialize(statement, @row : Array(T))
    super(statement)
    @index = 0
  end

  def move_next
    @index = 0
    true
  end

  def column_count : Int32
    @row.size
  end

  def column_name(index : Int32) : String
    index.to_s
  end

  def column_type(index : Int32)
    @row[index].class
  end

  {% for t in DB::TYPES %}
    # Reads the next column as a nillable {{t}}.
    def read?(t : {{t}}.class) : {{t}}?
      @index += 1
      @row[@index - 1] as {{t}}?
    end
  {% end %}
end

class FooDriver < DB::Driver
  @@row = [] of DB::Any

  def self.fake_row=(row : Array(DB::Any))
    @@row = row
  end

  def self.fake_row
    @@row
  end

  def build_connection(db : DB::Database) : DB::Connection
    FooConnection.new(db)
  end

  class FooConnection < DB::Connection
    def build_statement(query)
      FooStatement.new(self)
    end
  end

  class FooStatement < DB::Statement
    protected def perform_query(args : Slice(DB::Any)) : DB::ResultSet
      GenericResultSet(DB::Any).new(self, FooDriver.fake_row)
    end

    protected def perform_exec(args : Slice(DB::Any)) : DB::ExecResult
      raise "Not implemented"
    end
  end
end

DB.register_driver "foo", FooDriver

class BarDriver < DB::Driver
  @@row = [] of DB::Any

  def self.fake_row=(row : Array(DB::Any))
    @@row = row
  end

  def self.fake_row
    @@row
  end

  def build_connection(db : DB::Database) : DB::Connection
    BarConnection.new(db)
  end

  class BarConnection < DB::Connection
    def build_statement(query)
      BarStatement.new(self)
    end
  end

  class BarStatement < DB::Statement
    protected def perform_query(args : Slice(DB::Any)) : DB::ResultSet
      GenericResultSet(DB::Any).new(self, BarDriver.fake_row)
    end

    protected def perform_exec(args : Slice(DB::Any)) : DB::ExecResult
      raise "Not implemented"
    end
  end
end

DB.register_driver "bar", BarDriver

describe DB do
  it "should be able to register multiple drivers" do
    DB.open("foo://host").driver.should be_a(FooDriver)
    DB.open("bar://host").driver.should be_a(BarDriver)
  end

  it "Foo and Bar drivers should return fake_row" do
    with_witness do |w|
      DB.open("foo://host") do |db|
        FooDriver.fake_row = [1, "string"] of DB::Any
        db.query "query" do |rs|
          w.check
          rs.move_next
          rs.read?(Int32).should eq(1)
          rs.read?(String).should eq("string")
        end
      end
    end

    with_witness do |w|
      DB.open("bar://host") do |db|
        BarDriver.fake_row = ["lorem", 1.0] of DB::Any
        db.query "query" do |rs|
          w.check
          rs.move_next
          rs.read?(String).should eq("lorem")
          rs.read?(Float64).should eq(1.0)
        end
      end
    end
  end
end
