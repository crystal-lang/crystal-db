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

class ClosableWithSignal < Closable
  setter signal : Channel(Nil)?

  def initialize(@signal = nil)
  end

  protected def do_close
    @signal.try &.send(nil)
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
    pool = create_pool(max_pool_size: 2, max_idle_pool_size: 1, max_lifetime_per_resource: 0.1) { Closable.new.tap { |c| all << c } }

    sleep 0.1.seconds
    ex = expect_raises DB::PoolResourceLifetimeExpired(Closable) do
      pool.checkout
    end

    # Lifetime expiration error should cause the client to be closed
    all[0].closed?.should be_true
  end

  it "should expire resources that exceed maximum idle-time on checkout" do
    all = [] of Closable
    pool = create_pool(max_pool_size: 2, max_idle_pool_size: 1, max_idle_time_per_resource: 0.1, max_lifetime_per_resource: 2.0) { Closable.new.tap { |c| all << c } }

    sleep 0.1.seconds

    # Idle expiration error should cause the client to be closed
    ex = expect_raises DB::PoolResourceIdleExpired(Closable) do
      pool.checkout
    end

    all[0].closed?.should be_true
  end

  it "should expire resources that exceed maximum lifetime on release" do
    all = [] of Closable
    pool = create_pool(
      max_pool_size: 2,
      max_idle_pool_size: 1,
      max_lifetime_per_resource: 0.2,
      max_idle_time_per_resource: 2.0,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    pool.checkout { sleep 0.25.seconds }
    pool.stats.lifetime_expired_connections.should eq 1
    all[0].closed?.should be_true
  end

  it "should reset idle-time during release" do
    all = [] of Closable
    pool = create_pool(
      max_pool_size: 2,
      initial_pool_size: 0,
      max_idle_pool_size: 1,
      max_idle_time_per_resource: 0.2,
      max_lifetime_per_resource: 0.4,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    # Resource gets idled every 0.2 seconds but gets reset upon each release.
    pool.checkout do
      sleep 0.21.seconds
    end

    all[0].closed?.should be_false
    pool.checkout { sleep 0.2.seconds }

    pool.stats.lifetime_expired_connections.should eq(1)
    pool.stats.idle_expired_connections.should eq(0)
    all[0].closed?.should be_true
    all.size.should eq(1)
  end

  it "Should ensure minimum of initial_pool_size non-expired idle resources on checkout" do
    all = [] of Closable

    pool = create_pool(
      initial_pool_size: 3,
      max_pool_size: 5,
      max_idle_pool_size: 5,
      max_lifetime_per_resource: 1.0,
      max_idle_time_per_resource: 0.1,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    # Initially have 3 clients
    all.size.should eq(3)

    sleep 0.1.seconds

    # checkout
    3.times do |i|
      # Each call should cause another resource to be spawned and the old one expired off
      # for a minimum of three
      ex = expect_raises DB::PoolResourceIdleExpired(Closable) do
        pool.checkout { }
      end

      pool.stats.idle_connections.should eq(3)
      pool.stats.idle_expired_connections.should eq(i + 1)
      all.size.should eq(3 + (i + 1))
      all[i].closed?.should be_true
    end
  end

  it "Should ensure minimum of initial_pool_size non-expired idle resources on release" do
    all = [] of Closable

    pool = create_pool(
      initial_pool_size: 3,
      max_pool_size: 5,
      max_idle_pool_size: 5,
      max_lifetime_per_resource: 0.2,
      expired_resource_sweeper: false
    ) { Closable.new.tap { |c| all << c } }

    temp_resource_store = {
      pool.checkout,
      pool.checkout,
      pool.checkout,
    }

    # Await lifetime expiration
    sleep 0.2.seconds
    # release
    temp_resource_store.each_with_index do |resource, i|
      # All three idle connections were checked out
      # Each iteration should result in a new idle connection being created
      # as the one we release get expired.
      pool.release(resource)

      pool.stats.idle_connections.should eq(i + 1)
      pool.stats.lifetime_expired_connections.should eq(i + 1)
      all.size.should eq(3 + (i + 1))
      all[i].closed?.should be_true
    end
  end

  it "Should count inflight resources when ensuring minimum of initial_pool_size non-expired resources" do
    number_of_factory_calls = 0
    toggle_long_inflight = false

    close_inflight = Channel(Nil).new
    resource_closed_signal = Channel(Nil).new

    pool = create_pool(
      initial_pool_size: 3,
      max_pool_size: 5,
      max_idle_pool_size: 5,
      max_lifetime_per_resource: 0.25,
      expired_resource_sweeper: false
    ) do
      number_of_factory_calls += 1
      if toggle_long_inflight
        close_inflight.send(nil)
      end
      ClosableWithSignal.new(resource_closed_signal)
    end

    toggle_long_inflight = true
    temporary_latch = {pool.checkout, pool.checkout, pool.checkout}
    spawn { pool.checkout { } }

    # Make existing resources stale
    sleep 0.25.seconds

    pool.stats.idle_connections.should eq(0)
    pool.stats.in_flight_connections.should eq(1)

    # Release latched resources
    temporary_latch.each do |resource|
      spawn do
        pool.release(resource)
      end
    end

    # If inflight number is used correctly there should only be a total of
    # three new pending resources created in total which is used to replace the
    # expiring ones.
    #
    # +1 from the initial checkout (total 3, inflight: 1)
    # +0 from the first release (total 2, inflight: 1)
    # +1 from the second release (total: 1, inflight: 2)
    # +1 from the third release (total: 0, inflight: 3)

    3.times do
      resource_closed_signal.receive
      close_inflight.receive
    end

    # Should close gracefully and without any errors.
    close_inflight.close

    number_of_factory_calls.should eq(6)
    pool.stats.idle_connections.should eq(3)
    pool.stats.open_connections.should eq(3)
    pool.stats.in_flight_connections.should eq(0)
    pool.stats.lifetime_expired_connections.should eq(3)
  end

  describe "background expired resource sweeper " do
    it "should clear idle resources" do
      all = [] of Closable
      signal = Channel(Nil).new
      pool = create_pool(
        initial_pool_size: 0,
        max_pool_size: 5,
        max_idle_pool_size: 5,
        max_idle_time_per_resource: 0.25,
      ) do
        ClosableWithSignal.new.tap { |c| all << c }.tap &.signal = signal
      end

      # Create 5 resource
      5.times {
        spawn do
          pool.checkout { sleep 0.1.seconds }
        end
      }

      all.each &.closed?.should be_false

      5.times do
        signal.receive
      end

      # Gone
      all.each &.closed?.should be_true
      pool.stats.open_connections.should eq(0)
      pool.stats.idle_connections.should eq(0)
    end

    it "should ensure minimum of initial_pool_size fresh resources" do
      all = [] of Closable
      signal = Channel(Nil).new
      pool = create_pool(
        initial_pool_size: 3,
        max_pool_size: 5,
        max_idle_pool_size: 5,
        max_lifetime_per_resource: 2.0,
        max_idle_time_per_resource: 0.5,
      ) { ClosableWithSignal.new.tap { |c| all << c }.tap &.signal = signal }

      # The job will replace the three idle expired resources with new ones
      3.times { signal.receive }
      # Wait for the replenishment process to finish
      sleep 0.25.seconds

      all.size.should eq(6)
      all[..2].each &.closed?.should be_true
      all[3..].each &.closed?.should be_false
      pool.stats.idle_connections.should eq(3)
    end
  end
end
