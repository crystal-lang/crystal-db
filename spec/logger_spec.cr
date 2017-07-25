require "./spec_helper"

describe DB::Logger do
  it "logger class should exist" do
    DB::Logger.should_not be_nil
  end

  it "should work without logger setup" do
    with_dummy do |db|
      db.query("a query for logging")
    end
  end

  it "should callback to logger" do
    with_dummy do |db|
      test = ""
      db.logger = ->(sql : String) {
        test = sql
      }
      db.logging = true
      db.query("a query for logging")
      test.should eq(%{QUERY "a query for logging"})
      db.logging = false
    end
  end

  it "should no log to when logging=false" do
    with_dummy do |db|
      test = ""
      db.logger = ->(sql : String) {
        test = sql
      }
      db.logging = false
      db.query("a query for logging")
      test.should eq("")
    end
  end

  it "should log to std out" do
    with_dummy do |db|
      test = ""
      db.logger = nil
      db.logging = true
      db.query("a query for logging ?,?", 1, "Hello")
      test.should eq("")
    end
  end

  it "should log with array of params" do
    with_dummy do |db|
      test = ""
      test_args = nil
      db.logger = ->(log : String) {
        test = log
      }
      db.logging = true
      db.query("a query for logging ?,?", [1, "Hello"])
      test.should eq(%{QUERY "a query for logging ?,?" WITH PARAMS: [1, "Hello"]})
    end
  end

  it "should log with splat of params" do
    with_dummy do |db|
      test = ""
      test_args = nil
      db.logger = ->(log : String) {
        test = log
      }
      db.logging = true
      db.query("a query for logging ?,?", 1, "Hi")
      test.should eq(%{QUERY "a query for logging ?,?" WITH PARAMS: 1, Hi})
    end
  end
end
