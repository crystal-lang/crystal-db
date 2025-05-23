require "./spec_helper"
require "../src/db/error.cr"

class ShouldSleepingOp
  @is_sleeping = false
  getter is_sleeping
  getter sleep_happened

  def initialize
    @sleep_happened = Channel(Nil).new
  end

  def should_sleep(&)
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

private def create_pool(**options, &factory : -> T) forall T
  DB::Pool.new(DB::Pool::Options.new(**options), &factory)
end

describe DB::Pool do
  it "should use proc to create objects" do
    block_called = 0
    pool = create_pool(initial_pool_size: 3) { block_called += 1; Closable.new }
    block_called.should eq(3)
  end

  it "should get resource" do
    pool = create_pool { Closable.new }
    resource = pool.checkout
    resource.should be_a Closable
    resource.before_checkout_called.should be_true
  end

  it "should be available if not checkedout" do
    resource = uninitialized Closable
    pool = create_pool(initial_pool_size: 1) { resource = Closable.new }
    pool.is_available?(resource).should be_true
  end

  it "should not be available if checkedout" do
    pool = create_pool { Closable.new }
    resource = pool.checkout
    pool.is_available?(resource).should be_false
  end

  it "should be available if returned" do
    pool = create_pool { Closable.new }
    resource = pool.checkout
    resource.after_release_called.should be_false
    pool.release resource
    pool.is_available?(resource).should be_true
    resource.after_release_called.should be_true
  end

  it "should wait for available resource" do
    pool = create_pool(max_pool_size: 1, initial_pool_size: 1) { Closable.new }

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
    pool = create_pool(max_pool_size: 2, initial_pool_size: 1) { block_called += 1; Closable.new }
    block_called.should eq 1
    pool.checkout
    block_called.should eq 1
    pool.checkout
    block_called.should eq 2
  end

  it "should reuse returned resources" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 2, initial_pool_size: 1) { Closable.new.tap { |c| all << c } }
    pool.checkout
    b1 = pool.checkout
    pool.release b1
    b2 = pool.checkout

    b1.should eq b2
    all.size.should eq 2
  end

  it "should close available and total" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 2, initial_pool_size: 1) { Closable.new.tap { |c| all << c } }
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
    pool = create_pool(max_pool_size: 1, checkout_timeout: 0.1) { Closable.new }
    pool.checkout
    expect_raises DB::PoolTimeout do
      pool.checkout
    end
  end

  it "should be able to release after a timeout" do
    pool = create_pool(max_pool_size: 1, checkout_timeout: 0.1) { Closable.new }
    a = pool.checkout
    pool.checkout rescue nil
    pool.release a
  end

  it "should close if max idle amount is reached" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 3, max_idle_pool_size: 1) { Closable.new.tap { |c| all << c } }
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
    pool = create_pool(max_pool_size: 1, max_idle_pool_size: 1) { Closable.new }

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
    pool = create_pool(max_pool_size: 3, max_idle_pool_size: 1) { Closable.new.tap { |c| all << c } }
    pool.checkout
    pool.checkout
    pool.checkout
    pool.release all[0]
    pool.release all[1]
    pool.checkout
    pool.checkout

    all.size.should eq 4
  end

  it "should expire resources that exceed maximum lifetime on checkout" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 2, max_idle_pool_size: 1, max_lifetime_per_resource: 0.5) { Closable.new.tap { |c| all << c } }

    # After 0.5 seconds we should expect to get an expired resource
    sleep 0.5.seconds

    ex = expect_raises DB::PoolResourceLifetimeExpired(Closable) do
      pool.checkout
    end

    # Lifetime expiration error should cause the client to be closed
    all[0].closed?.should be_true
  end

  it "should expire resources that exceed maximum idle-time on checkout" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 2, max_idle_pool_size: 1, max_idle_time_per_resource: 0.5) { Closable.new.tap { |c| all << c } }

    # After two seconds we should expect to get an expired resource
    sleep 0.5.seconds

    # Idle expiration error should cause the client to be closed
    ex = expect_raises DB::PoolResourceIdleExpired(Closable) do
      pool.checkout
    end

    all[0].closed?.should be_true
  end

  it "should only check lifetime expiration on release" do
    all = [] of Closable
    pool = create_pool(
      max_pool_size: 2,
      max_idle_pool_size: 1,
      max_lifetime_per_resource: 2.0,
      max_idle_time_per_resource: 0.5,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    # Idle gets reset; not expired
    pool.checkout do |client|
      sleep 0.5.seconds
    end

    # Not closed?
    all[0].closed?.should be_false

    # We should expect to see an idle connection timeout now with the #checkout after
    # waiting another 0.5 seconds

    sleep 0.6.seconds
    ex = expect_raises DB::PoolResourceIdleExpired(Closable) do
      pool.checkout
    end

    all[0].closed?.should be_true

    # This should now create a new client that will be expired on release
    ex = expect_raises DB::PoolResourceLifetimeExpired(Closable) do
      pool.checkout { sleep 2.seconds }
    end

    all[1].closed?.should be_true
  end

  it "should reset idle-time on checkout" do
    all = [] of Closable
    pool = create_pool(
      max_pool_size: 2,
      max_idle_pool_size: 1,
      max_idle_time_per_resource: 1.0,
      max_lifetime_per_resource: 2.0,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    # Resource gets idled every second. It should get reset every time we checkout and release the resource.
    # If we can last more than 2 seconds from the time of creation then it should get expired
    # by the lifetime expiration instead.

    # Idle expiration error should cause the resource to be closed
    ex = expect_raises DB::PoolResourceLifetimeExpired(Closable) do
      2.times {
        pool.checkout {
          sleep 1
        }
      }
    end

    all[0].closed?.should be_true
  end

  describe "background expired resource sweeper " do
    it "should clear idle resources" do
      all = [] of Closable
      pool = create_pool(
        initial_pool_size: 0,
        max_pool_size: 5,
        max_idle_pool_size: 5,
        max_idle_time_per_resource: 2.0,
      ) { Closable.new.tap { |c| all << c } }

      # Create 5 resource
      5.times {
        spawn do
          pool.checkout { sleep 0.1 }
        end
      }

      # Don't do anything for 5 seconds
      sleep 5

      # Gone
      all.each &.closed?.should be_true
      pool.stats.open_connections.should eq(0)
      pool.stats.idle_connections.should eq(0)
    end

    it "should ensure minimum of initial_pool_size fresh resources" do
      all = [] of Closable
      pool = create_pool(
        initial_pool_size: 3,
        max_pool_size: 5,
        max_idle_pool_size: 5,
        max_lifetime_per_resource: 2.0,
        max_idle_time_per_resource: 0.5,
      ) { Closable.new.tap { |c| all << c } }

      # Since `resource_sweeper_timer` we should expect to see a sweep every 0.5 seconds (idle is lowest expiration)
      #
      # The first run occurs after 0.5 seconds which mean that the initial 3 resources should've gotten sweeped.
      # Then three more resources should be created as to ensure that the amount of young resources within the pool
      # never goes below initial_pool_size. We should be left with 6 clients created in total.
      #
      # A whole second to ensure the sweep fiber's completion.
      sleep 1.seconds

      all.size.should eq(6)
      all[..2].each &.closed?.should be_true
      all[3..].each &.closed?.should be_false
      pool.stats.idle_connections.should eq(3)
    end
  end
end
