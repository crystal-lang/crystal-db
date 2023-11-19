require "./spec_helper"
require "log/spec"

describe DB::Statement do
  it "should build prepared statements" do
    with_dummy_connection do |cnn|
      prepared = cnn.prepared("the query")
      prepared.should be_a(DB::Statement)
      prepared.as(DummyDriver::DummyStatement).prepared?.should be_true
    end
  end

  it "should build unprepared statements" do
    with_dummy_connection("prepared_statements=false") do |cnn|
      prepared = cnn.unprepared("the query")
      prepared.should be_a(DB::Statement)
      prepared.as(DummyDriver::DummyStatement).prepared?.should be_false
    end
  end

  describe "prepared_statements flag" do
    it "should build prepared statements if true" do
      with_dummy_connection("prepared_statements=true") do |cnn|
        stmt = cnn.query("the query").statement
        stmt.as(DummyDriver::DummyStatement).prepared?.should be_true
      end
    end

    it "should build unprepared statements if false" do
      with_dummy_connection("prepared_statements=false") do |cnn|
        stmt = cnn.query("the query").statement
        stmt.as(DummyDriver::DummyStatement).prepared?.should be_false
      end
    end
  end

  describe "prepared_statements_cache flag" do
    it "should reuse prepared statements if true" do
      with_dummy_connection("prepared_statements=true&prepared_statements_cache=true") do |cnn|
        stmt1 = cnn.query("the query").statement
        stmt2 = cnn.query("the query").statement
        stmt1.object_id.should eq(stmt2.object_id)
      end
    end

    it "should leave statements open to be reused if true" do
      with_dummy_connection("prepared_statements=true&prepared_statements_cache=true") do |cnn|
        rs = cnn.query("the query")
        # do not close while iterating
        rs.statement.closed?.should be_false
        rs.close
        # do not close to be reused
        rs.statement.closed?.should be_false
      end
    end

    it "should not reuse prepared statements if false" do
      with_dummy_connection("prepared_statements=true&prepared_statements_cache=false") do |cnn|
        stmt1 = cnn.query("the query").statement
        stmt2 = cnn.query("the query").statement
        stmt1.object_id.should_not eq(stmt2.object_id)
      end
    end

    it "should close statements if false" do
      with_dummy_connection("prepared_statements=true&prepared_statements_cache=false") do |cnn|
        rs = cnn.query("the query")
        # do not close while iterating
        rs.statement.closed?.should be_false
        rs.close
        # do close after iterating
        rs.statement.closed?.should be_true
      end
    end

    it "should not close statements if false and created explicitly" do
      with_dummy_connection("prepared_statements=true&prepared_statements_cache=false") do |cnn|
        stmt = cnn.prepared("the query")

        rs = stmt.query
        # do not close while iterating
        stmt.closed?.should be_false
        rs.close

        # do not close after iterating
        stmt.closed?.should be_false
      end
    end
  end

  it "should initialize positional params in query" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.query "a", 1, nil
      stmt.params[0].should eq("a")
      stmt.params[1].should eq(1)
      stmt.params[2].should eq(nil)
    end
  end

  it "accepts array as single argument" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.query ["a", 1, nil]
      stmt.params[0].should eq(["a", 1, nil])
    end
  end

  it "allows no arguments" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.query
      stmt.params.should be_empty
    end
  end

  it "concatenate arguments" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.query 1, 2, args: ["a", [1, nil]]
      stmt.params[0].should eq(1)
      stmt.params[1].should eq(2)
      stmt.params[2].should eq("a")
      stmt.params[3].should eq([1, nil])
    end
  end

  it "should initialize positional params in query with array" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.query args: ["a", 1, nil]
      stmt.params[0].should eq("a")
      stmt.params[1].should eq(1)
      stmt.params[2].should eq(nil)
    end
  end

  it "should initialize positional params in exec" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.exec "a", 1, nil
      stmt.params[0].should eq("a")
      stmt.params[1].should eq(1)
      stmt.params[2].should eq(nil)
    end
  end

  it "accepts array as single argument" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.exec ["a", 1, nil]
      stmt.params[0].should eq(["a", 1, nil])
    end
  end

  it "should initialize positional params in exec with array" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.exec args: ["a", 1, nil]
      stmt.params[0].should eq("a")
      stmt.params[1].should eq(1)
      stmt.params[2].should eq(nil)
    end
  end

  it "allows no arguments" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.exec
      stmt.params.should be_empty
    end
  end

  it "concatenate arguments" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.exec 1, 2, args: ["a", [1, nil]]
      stmt.params[0].should eq(1)
      stmt.params[1].should eq(2)
      stmt.params[2].should eq("a")
      stmt.params[3].should eq([1, nil])
    end
  end

  it "should initialize positional params in scalar" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
      stmt.scalar "a", 1, nil
      stmt.params[0].should eq("a")
      stmt.params[1].should eq(1)
      stmt.params[2].should eq(nil)
    end
  end

  it "query with block should not close statement" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.query
      stmt.closed?.should be_false
    end
  end

  it "closing connection should close statement" do
    stmt = uninitialized DB::Statement
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.query
    end
    stmt.closed?.should be_true
  end

  it "query with block should not close statement" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.query do |rs|
      end
      stmt.closed?.should be_false
    end
  end

  it "query should not close statement" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.query do |rs|
      end
      stmt.closed?.should be_false
    end
  end

  it "scalar should not close statement" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.scalar
      stmt.closed?.should be_false
    end
  end

  it "exec should not close statement" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared "3,4 1,2"
      stmt.exec
      stmt.closed?.should be_false
    end
  end

  it "connection should cache statements by query" do
    with_dummy_connection do |cnn|
      rs = cnn.prepared.query "1, ?", 2
      stmt = rs.statement
      rs.close

      rs = cnn.prepared.query "1, ?", 4
      rs.statement.should be(stmt)
    end
  end

  it "connection should be released if error occurs during exec" do
    with_dummy do |db|
      expect_raises DB::Error do
        db.exec "raise"
      end
      DummyDriver::DummyConnection.connections.size.should eq(1)
      db.pool.is_available?(DummyDriver::DummyConnection.connections.first)
    end
  end

  it "raises NoResultsError for scalar" do
    with_dummy_connection do |cnn|
      stmt = cnn.prepared ""
      expect_raises DB::NoResultsError do
        stmt.scalar "SELECT LIMIT 0"
      end
    end
  end

  describe "logging" do
    it "exec with no arguments" do
      Log.capture(DB::Log.source) do |logs|
        with_dummy do |db|
          db.exec "42"
        end

        entry = logs.check(:debug, /Executing query/i).entry
        entry.data[:query].should eq("42")
        entry.data[:args].as_a.should be_empty
      end
    end

    it "query with arguments" do
      Log.capture(DB::Log.source) do |logs|
        with_dummy do |db|
          db.exec "1, ?", args: ["a"]
          db.exec "2, ?", "a"
          db.exec "3, ?", ["a"]
        end

        entry = logs.check(:debug, /Executing query/i).entry
        entry.data[:query].should eq("1, ?")
        entry.data[:args][0].as_s.should eq("a")

        entry = logs.check(:debug, /Executing query/i).entry
        entry.data[:query].should eq("2, ?")
        entry.data[:args][0].as_s.should eq("a")

        entry = logs.check(:debug, /Executing query/i).entry
        entry.data[:query].should eq("3, ?")
        entry.data[:args][0][0].as_s.should eq("a")
      end
    end
  end
end
