require "./spec_helper"

class ShouldSleepingOp
  @is_sleeping = false
  getter is_sleeping
  getter sleep_happened

  def initialize
    @sleep_happened = Channel(Nil).new
  end

  def should_sleep
    s = self
    @is_sleeping = true
    spawn do
      sleep 0.1
      s.is_sleeping.should be_true
      s.sleep_happened.send(nil)
    end
    yield
    @is_sleeping = false
  end

  def wait_for_sleep
    @sleep_happened.receive
  end
end

class WaitFor
  def initialize
    @channel = Channel(Nil).new
  end

  def wait
    @channel.receive
  end

  def check
    @channel.send(nil)
  end
end

class Closable
  include DB::Disposable
  property before_checkout_called : Bool = false
  property after_release_called : Bool = false

  protected def do_close
  end

  def before_checkout
    @before_checkout_called = true
  end

  def after_release
    @after_release_called = true
  end
end

describe DB::Pool do
  it "should use proc to create objects" do
    block_called = 0
    pool = DB::Pool.new(initial_pool_size: 3) { block_called += 1; Closable.new }
    block_called.should eq(3)
  end

  it "should get resource" do
    pool = DB::Pool.new { Closable.new }
    resource = pool.checkout
    resource.should be_a Closable
    resource.before_checkout_called.should be_true
  end

  it "should be available if not checkedout" do
    resource = uninitialized Closable
    pool = DB::Pool.new(initial_pool_size: 1) { resource = Closable.new }
    pool.is_available?(resource).should be_true
  end

  it "should not be available if checkedout" do
    pool = DB::Pool.new { Closable.new }
    resource = pool.checkout
    pool.is_available?(resource).should be_false
  end

  it "should be available if returned" do
    pool = DB::Pool.new { Closable.new }
    resource = pool.checkout
    resource.after_release_called.should be_false
    pool.release resource
    pool.is_available?(resource).should be_true
    resource.after_release_called.should be_true
  end

  it "should wait for available resource" do
    pool = DB::Pool.new(max_pool_size: 1, initial_pool_size: 1) { Closable.new }

    b_cnn_request = ShouldSleepingOp.new
    wait_a = WaitFor.new
    wait_b = WaitFor.new

    spawn do
      a_cnn = pool.checkout
      b_cnn_request.wait_for_sleep
      pool.release a_cnn

      wait_a.check
    end

    spawn do
      b_cnn_request.should_sleep do
        pool.checkout
      end

      wait_b.check
    end

    wait_a.wait
    wait_b.wait
  end

  it "should create new if max was not reached" do
    block_called = 0
    pool = DB::Pool.new(max_pool_size: 2, initial_pool_size: 1) { block_called += 1; Closable.new }
    block_called.should eq 1
    pool.checkout
    block_called.should eq 1
    pool.checkout
    block_called.should eq 2
  end

  it "should reuse returned resources" do
    all = [] of Closable
    pool = DB::Pool.new(max_pool_size: 2, initial_pool_size: 1) { Closable.new.tap { |c| all << c } }
    pool.checkout
    b1 = pool.checkout
    pool.release b1
    b2 = pool.checkout

    b1.should eq b2
    all.size.should eq 2
  end

  it "should close available and total" do
    all = [] of Closable
    pool = DB::Pool.new(max_pool_size: 2, initial_pool_size: 1) { Closable.new.tap { |c| all << c } }
    a = pool.checkout
    b = pool.checkout
    pool.release b
    all.size.should eq 2

    all[0].closed?.should be_false
    all[1].closed?.should be_false
    pool.close
    all[0].closed?.should be_true
    all[1].closed?.should be_true
  end

  it "should timeout" do
    pool = DB::Pool.new(max_pool_size: 1, checkout_timeout: 0.1) { Closable.new }
    pool.checkout
    expect_raises DB::PoolTimeout do
      pool.checkout
    end
  end

  it "should be able to release after a timeout" do
    pool = DB::Pool.new(max_pool_size: 1, checkout_timeout: 0.1) { Closable.new }
    a = pool.checkout
    pool.checkout rescue nil
    pool.release a
  end

  it "should close if max idle amount is reached" do
    all = [] of Closable
    pool = DB::Pool.new(max_pool_size: 3, max_idle_pool_size: 1) { Closable.new.tap { |c| all << c } }
    pool.checkout
    pool.checkout
    pool.checkout

    all.size.should eq 3
    all.any?(&.closed?).should be_false
    pool.release all[0]

    all.any?(&.closed?).should be_false
    pool.release all[1]

    all[0].closed?.should be_false
    all[1].closed?.should be_true
    all[2].closed?.should be_false
  end

  it "should not return closed resources to the pool" do
    pool = DB::Pool.new(max_pool_size: 1, max_idle_pool_size: 1) { Closable.new }

    # pool size 1 should be reusing the one resource
    resource1 = pool.checkout
    pool.release resource1
    resource2 = pool.checkout
    resource1.should eq resource2

    # it should not return a closed resource to the pool
    resource2.close
    pool.release resource2

    resource2 = pool.checkout
    resource1.should_not eq resource2
  end

  it "should create resource after max_pool was reached if idle forced some close up" do
    all = [] of Closable
    pool = DB::Pool.new(max_pool_size: 3, max_idle_pool_size: 1) { Closable.new.tap { |c| all << c } }
    pool.checkout
    pool.checkout
    pool.checkout
    pool.release all[0]
    pool.release all[1]
    pool.checkout
    pool.checkout

    all.size.should eq 4
  end

  it "should close idle resources after a health check fails" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=1&reaping_delay=0.0&reaping_frequency=0.01" do |db|
      cnn = db.checkout.as(DummyDriver::DummyConnection)
      cnn.closed?.should be_false
      cnn.release
      db.pool.is_available?(cnn).should be_true
      db.pool.is_in_pool?(cnn).should be_true
      cnn.disconnect!
      sleep(0.02)
      db.pool.is_available?(cnn).should be_false
      db.pool.is_in_pool?(cnn).should be_false
    end
  end

  it "should not close unhealthy resources until after the reaping_delay" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=1&reaping_delay=0.05&reaping_frequency=0.01" do |db|
      cnn = db.checkout.as(DummyDriver::DummyConnection)
      cnn.closed?.should be_false
      cnn.release
      db.pool.is_available?(cnn).should be_true
      db.pool.is_in_pool?(cnn).should be_true
      cnn.disconnect!
      sleep(0.025)
      db.pool.is_available?(cnn).should be_true
      db.pool.is_in_pool?(cnn).should be_true
      sleep(0.125)
      db.pool.is_available?(cnn).should be_false
      db.pool.is_in_pool?(cnn).should be_false
    end
  end

  it "should run health checks consecutively" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=2&max_pool_size=2&max_idle_pool_size=2&reaping_delay=0.0&reaping_frequency=0.01" do |db|
      cnn = db.checkout.as(DummyDriver::DummyConnection)
      cnn.simulated_health_check_delay = 0.1
      cnn2 = db.checkout.as(DummyDriver::DummyConnection)
      cnn.release
      cnn2.release
      cnn.disconnect!
      cnn2.disconnect!
      db.pool.is_available?(cnn).should be_true
      db.pool.is_in_pool?(cnn).should be_true
      db.pool.is_available?(cnn2).should be_true
      db.pool.is_in_pool?(cnn2).should be_true
      sleep(0.02)
      db.pool.is_available?(cnn).should be_false
      db.pool.is_in_pool?(cnn).should be_false
      db.pool.is_available?(cnn2).should be_true
      db.pool.is_in_pool?(cnn2).should be_true
      sleep(0.02)
      db.pool.is_available?(cnn2).should be_true
      db.pool.is_in_pool?(cnn2).should be_true
      sleep(0.2)
      db.pool.is_available?(cnn2).should be_false
      db.pool.is_in_pool?(cnn2).should be_false
    end
  end

  it "should not close open resources" do
    DummyDriver::DummyConnection.clear_connections
    DB.open "dummy://localhost:1027?initial_pool_size=1&max_pool_size=2&reaping_delay=0.0&reaping_frequency=0.01" do |db|
      cnn = db.checkout.as(DummyDriver::DummyConnection)
      cnn2 = db.checkout.as(DummyDriver::DummyConnection)
      cnn.release
      cnn.disconnect!
      cnn2.disconnect!
      db.pool.is_available?(cnn).should be_true
      db.pool.is_in_pool?(cnn).should be_true
      db.pool.is_available?(cnn2).should be_false
      db.pool.is_in_pool?(cnn2).should be_true
      sleep(0.02)
      db.pool.is_available?(cnn).should be_false
      db.pool.is_in_pool?(cnn).should be_false
      db.pool.is_available?(cnn2).should be_false
      db.pool.is_in_pool?(cnn2).should be_true
    end
  end
end
