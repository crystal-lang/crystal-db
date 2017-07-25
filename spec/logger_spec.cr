require "./spec_helper"

describe DB::Logger do
  # it "should initialize positional params in query" do
  #     with_dummy_connection do |cnn|
  #       stmt = cnn.prepared("the query").as(DummyDriver::DummyStatement)
  #       stmt.query "a", 1, nil
  #       stmt.params[0].should eq("a")
  #       stmt.params[1].should eq(1)
  #       stmt.params[2].should eq(nil)
  #     end
  #   end

  it "logger class should exist" do
    DB::Logger.should_not be_nil
  end

  it "should work without logger setup" do
    with_dummy do |db|
      db.query("a query for logging")
     end
  end

  it "should callback to logger" do
     test = ""
     DB::Logger.config ->(sql : String){
       test=sql
     } 
     DB::Logger.logging = true
     with_dummy do |db|
      db.query("a query for logging")
     end
     test.should eq("a query for logging")
  end

  it "should disabled logger if logging is false" do
     test = ""
     DB::Logger.logging = false
     DB::Logger.config logger: ->(sql : String){ test=sql } 
     with_dummy do |db|
      db.query("a query for logging")
     end
     test.should eq("")
  end
 
  it "should disabled logger if set to nil" do
     test = ""
     DB.logging = true
     DB::Logger.config logger: ->(sql : String){
       test=sql
     } 
     with_dummy do |db|
      db.query("a query for logging")
     end
     test.should eq("a query for logging")

     test = ""
     DB::Logger.config(logger: nil)
     with_dummy do |db|
      db.query("a query for logging")
     end
     test.should eq("")
  end

  it "should work with shorter syntax" do 
     test = ""
     DB.logger = ->(sql : String){ 
       test=sql 
     } 
     DB.logging = true
     with_dummy do |db|
      db.query("a simpler logging")
     end
     test.should eq("a simpler logging")
     DB.logging = false
    
     test = ""
     with_dummy do |db|
      db.query("a simpler logging")
     end
     test.should eq("")
  end
    
end
