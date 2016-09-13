require "./spec_helper"

describe DB::Database do
  it "allows connection initialization" do
    cnn_setup = 0
    DB.open "dummy://localhost:1027?initial_pool_size=2&max_pool_size=4&max_idle_pool_size=1" do |db|
      cnn_setup.should eq(0)

      db.setup_connection do |cnn|
        cnn_setup += 1
      end

      cnn_setup.should eq(2)

      db.using_connection do
        cnn_setup.should eq(2)
        db.using_connection do
          cnn_setup.should eq(2)
          db.using_connection do
            cnn_setup.should eq(3)
            db.using_connection do
              cnn_setup.should eq(4)
            end
            # the pool didn't shrink no new initialization should be done next
            db.using_connection do
              cnn_setup.should eq(4)
            end
          end
          # the pool shrink 1. max_idle_pool_size=1
          # after the previous end there where 2.
          db.using_connection do
            cnn_setup.should eq(4)
            # so now there will be a new connection created
            db.using_connection do
              cnn_setup.should eq(5)
            end
          end
        end
      end
    end
  end

  it "should allow creation of more statements than pool connections" do
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=2" do |db|
      db.prepare("query1").should be_a(DB::PoolStatement)
      db.prepare("query2").should be_a(DB::PoolStatement)
      db.prepare("query3").should be_a(DB::PoolStatement)
    end
  end

  it "should return same statement in pool per query" do
    with_dummy do |db|
      stmt = db.prepare("query1")
      db.prepare("query2").should_not eq(stmt)
      db.prepare("query1").should eq(stmt)
    end
  end

  it "should close pool statements when closing db" do
    stmt = uninitialized DB::PoolStatement
    with_dummy do |db|
      stmt = db.prepare("query1")
    end
    stmt.closed?.should be_true
  end

  it "should not reconnect if connection is lost and retry_attempts=0" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=1&retry_attempts=0" do |db|
      db.exec("stmt1")
      DummyDriver::DummyConnection.connections.size.should eq(1)
      DummyDriver::DummyConnection.connections.first.disconnect!
      expect_raises DB::PoolRetryAttemptsExceeded do
        db.exec("stmt1")
      end
      DummyDriver::DummyConnection.connections.size.should eq(1)
    end
  end

  it "should reconnect if connection is lost and executing same statement" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=1&retry_attempts=1" do |db|
      db.exec("stmt1")
      DummyDriver::DummyConnection.connections.size.should eq(1)
      DummyDriver::DummyConnection.connections.first.disconnect!
      db.exec("stmt1")
      DummyDriver::DummyConnection.connections.size.should eq(2)
    end
  end
end
