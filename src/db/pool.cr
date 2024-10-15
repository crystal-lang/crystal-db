require "weak_ref"

require "./error"

module DB
  class Pool(T)
    record Options,
      # initial number of connections in the pool
      initial_pool_size : Int32 = 1,
      # maximum amount of connections in the pool (Idle + InUse). 0 means no maximum.
      max_pool_size : Int32 = 0,
      # maximum amount of idle connections in the pool
      max_idle_pool_size : Int32 = 1,
      # seconds to wait before timeout while doing a checkout
      checkout_timeout : Float64 = 5.0,
      # maximum amount of retry attempts to reconnect to the db. See `Pool#retry`
      retry_attempts : Int32 = 1,
      # seconds to wait before a retry attempt
      retry_delay : Float64 = 0.2 do
      def self.from_http_params(params : HTTP::Params, default = Options.new)
        Options.new(
          initial_pool_size: params.fetch("initial_pool_size", default.initial_pool_size).to_i,
          max_pool_size: params.fetch("max_pool_size", default.max_pool_size).to_i,
          max_idle_pool_size: params.fetch("max_idle_pool_size", default.max_idle_pool_size).to_i,
          checkout_timeout: params.fetch("checkout_timeout", default.checkout_timeout).to_f,
          retry_attempts: params.fetch("retry_attempts", default.retry_attempts).to_i,
          retry_delay: params.fetch("retry_delay", default.retry_delay).to_f,
        )
      end
    end

    # Pool configuration

    # initial number of connections in the pool
    @initial_pool_size : Int32
    # maximum amount of connections in the pool (Idle + InUse)
    @max_pool_size : Int32
    # maximum amount of idle connections in the pool
    @max_idle_pool_size : Int32
    # seconds to wait before timeout while doing a checkout
    @checkout_timeout : Float64
    # maximum amount of retry attempts to reconnect to the db. See `Pool#retry`
    @retry_attempts : Int32
    # seconds to wait before a retry attempt
    @retry_delay : Float64

    # Pool state

    # total of open connections managed by this pool
    @total = [] of T
    # connections available for checkout
    @idle = Set(T).new
    # connections waiting to be stablished (they are not in *@idle* nor in *@total*)
    @inflight : Int32

    # Sync state

    # communicate that a connection is available for checkout
    @availability_channel : Channel(Nil)
    # global pool mutex
    @mutex : Mutex

    @[Deprecated("Use `#new` with DB::Pool::Options instead")]
    def initialize(initial_pool_size = 1, max_pool_size = 0, max_idle_pool_size = 1, checkout_timeout = 5.0,
                   retry_attempts = 1, retry_delay = 0.2, &factory : -> T)
      initialize(
        Options.new(
          initial_pool_size: initial_pool_size, max_pool_size: max_pool_size,
          max_idle_pool_size: max_idle_pool_size, checkout_timeout: checkout_timeout,
          retry_attempts: retry_attempts, retry_delay: retry_delay),
        &factory)
    end

    def initialize(pool_options : Options = Options.new, &@factory : -> T)
      @initial_pool_size = pool_options.initial_pool_size
      @max_pool_size = pool_options.max_pool_size
      @max_idle_pool_size = pool_options.max_idle_pool_size
      @checkout_timeout = pool_options.checkout_timeout
      @retry_attempts = pool_options.retry_attempts
      @retry_delay = pool_options.retry_delay

      @availability_channel = Channel(Nil).new
      @inflight = 0
      @mutex = Mutex.new

      @initial_pool_size.times { build_resource }
    end

    # close all resources in the pool
    def close : Nil
      @total.each &.close
      @total.clear
      @idle.clear
    end

    record Stats,
      open_connections : Int32,
      idle_connections : Int32,
      in_flight_connections : Int32,
      max_connections : Int32

    # Returns stats of the pool
    def stats
      Stats.new(
        open_connections: @total.size,
        idle_connections: @idle.size,
        in_flight_connections: @inflight,
        max_connections: @max_pool_size,
      )
    end

    def checkout : T
      res = sync do
        resource = nil

        until resource
          resource = if @idle.empty?
                       if can_increase_pool?
                         @inflight += 1
                         begin
                           r = unsync { build_resource }
                         ensure
                           @inflight -= 1
                         end
                         r
                       else
                         unsync { wait_for_available }
                         # The wait for available can unlock
                         # multiple fibers waiting for a resource.
                         # Although only one will pick it due to the lock
                         # in the end of the unsync, the pick_available
                         # will return nil
                         pick_available
                       end
                     else
                       pick_available
                     end
        end

        @idle.delete resource

        resource
      end

      if res.responds_to?(:before_checkout)
        res.before_checkout
      end
      res
    end

    def checkout(&block : T ->)
      connection = checkout

      begin
        yield connection
      ensure
        release connection
      end
    end

    def release(resource : T) : Nil
      idle_pushed = false

      sync do
        if resource.responds_to?(:closed?) && resource.closed?
          @total.delete(resource)
        elsif can_increase_idle_pool
          @idle << resource
          if resource.responds_to?(:after_release)
            resource.after_release
          end
          idle_pushed = true
        else
          resource.close
          @total.delete(resource)
        end
      end

      if idle_pushed
        select
        when @availability_channel.send(nil)
        else
        end
      end
    end

    # :nodoc:
    # Will retry the block if a `ConnectionLost` exception is thrown.
    # It will try to reuse all of the available connection right away,
    # but if a new connection is needed there is a `retry_delay` seconds delay.
    def retry(&)
      current_available = 0

      sync do
        current_available = @idle.size
        # if the pool hasn't reach the max size, allow 1 attempt
        # to make a new connection if needed without sleeping
        current_available += 1 if can_increase_pool?
      end

      (current_available + @retry_attempts).times do |i|
        begin
          sleep @retry_delay if i >= current_available
          return yield
        rescue e : PoolResourceLost(T)
          # if the connection is lost it will be closed by
          # the exception to release resources
          # we still need to remove it from the known pool.
          sync { delete(e.resource) }
        rescue e : PoolResourceRefused
          # a ConnectionRefused means a new connection
          # was intended to be created,
          # nothing to do but to retry soon
        end
      end
      raise PoolRetryAttemptsExceeded.new
    end

    # :nodoc:
    def each_resource(&)
      sync do
        @idle.each do |resource|
          yield resource
        end
      end
    end

    # :nodoc:
    def is_available?(resource : T)
      @idle.includes?(resource)
    end

    # :nodoc:
    def delete(resource : T)
      @total.delete(resource)
      @idle.delete(resource)
    end

    private def build_resource : T
      resource = @factory.call
      sync do
        @total << resource
        @idle << resource
      end
      resource
    end

    private def can_increase_pool?
      @max_pool_size == 0 || @total.size + @inflight < @max_pool_size
    end

    private def can_increase_idle_pool
      @idle.size < @max_idle_pool_size
    end

    private def pick_available
      @idle.first?
    end

    private def wait_for_available
      select
      when @availability_channel.receive
      when timeout(@checkout_timeout.seconds)
        raise DB::PoolTimeout.new("Could not check out a connection in #{@checkout_timeout} seconds")
      end
    end

    private def sync(&)
      @mutex.lock
      begin
        yield
      ensure
        @mutex.unlock
      end
    end

    private def unsync(&)
      @mutex.unlock
      begin
        yield
      ensure
        @mutex.lock
      end
    end
  end
end
