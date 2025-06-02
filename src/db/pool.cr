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
      retry_delay : Float64 = 0.2,
      # maximum number of seconds the resource can persist after being created. 0 to disable.
      max_lifetime_per_resource : Float64 | Int32 | Time::Span = 0.0,
      # maximum number of seconds an idle resource can remain unused for being removed. 0 to disable.
      max_idle_time_per_resource : Float64 | Int32 | Time::Span = 0.0,
      # whether to enable a background sweeper to remove expired clients. Default is true but it will only be spawned if an expiration is actually set
      expired_resource_sweeper : Bool = true,
      # number of seconds to wait between each run of the expired resource sweeper. When unset (0) this value defaults to the shortest expiration duration
      resource_sweeper_timer : Float64 | Int32 | Time::Span = 0 do
      def self.from_http_params(params : HTTP::Params, default = Options.new)
        enabled_sweeper = params.fetch("expired_resource_sweeper", default.expired_resource_sweeper)
        if enabled_sweeper.is_a?(String)
          enabled_sweeper = {'1', "true", 't', "yes"}.includes?(enabled_sweeper.downcase)
        end

        Options.new(
          initial_pool_size: params.fetch("initial_pool_size", default.initial_pool_size).to_i,
          max_pool_size: params.fetch("max_pool_size", default.max_pool_size).to_i,
          max_idle_pool_size: params.fetch("max_idle_pool_size", default.max_idle_pool_size).to_i,
          checkout_timeout: params.fetch("checkout_timeout", default.checkout_timeout).to_f,
          retry_attempts: params.fetch("retry_attempts", default.retry_attempts).to_i,
          retry_delay: params.fetch("retry_delay", default.retry_delay).to_f,
          max_lifetime_per_resource: params.fetch("max_lifetime_per_resource", default.max_lifetime_per_resource).to_f,
          max_idle_time_per_resource: params.fetch("max_idle_time_per_resource", default.max_idle_time_per_resource).to_f,
          expired_resource_sweeper: enabled_sweeper,
          resource_sweeper_timer: params.fetch("resource_sweeper_timer", default.resource_sweeper_timer).to_f,
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

    # maximum number of seconds the resource can persist after being created. 0 to disable.
    @max_lifetime_per_resource : Time::Span
    # maximum number of seconds an idle resource can remain unused for being removed. 0 to disable.
    @max_idle_time_per_resource : Time::Span

    # Pool state

    # total of open connections managed by this pool
    @total = [] of T
    # connections available for checkout
    @idle = Set(T).new
    # connections waiting to be stablished (they are not in *@idle* nor in *@total*)
    @inflight : Int32

    @idle_expired_count : Int64 = 0
    @lifetime_expired_count : Int64 = 0

    # Tracks creation and last (checked out) used timestamps of a specific resource
    private class ResourceTimeEntry
      # Time of creation
      getter creation : Time = Time.utc
      # Time the resource was last checked out
      getter last_checked_out : Time

      def initialize
        @last_checked_out = @creation
      end

      # Sets the last checked out time to now
      def got_checked_out
        @last_checked_out = Time.utc
      end
    end

    # Maps a resource to a corresponding `ResourceTimeEntry`
    @resource_lifecycle = {} of T => ResourceTimeEntry

    # Sync state

    # communicate that a connection is available for checkout
    @availability_channel : Channel(Nil)
    # global pool mutex
    @mutex : Mutex

    # Sweep expired resource job

    # whether the job is enabled or disabled
    @sweep_job_enabled : Bool
    # has a sweep job running
    @sweep_job_running : Bool
    # timer between each run
    @sweep_timer : Time::Span?
    # cancels the sweep job as needed
    @sweep_job_close_channel : Channel(Nil)

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

      @max_lifetime_per_resource = ensure_time_span(pool_options.max_lifetime_per_resource).as(Time::Span)
      @max_idle_time_per_resource = ensure_time_span(pool_options.max_idle_time_per_resource).as(Time::Span)

      @initial_pool_size.times { build_resource }

      @sweep_job_enabled = pool_options.expired_resource_sweeper

      # Cancels the sweep job as needed
      @sweep_job_close_channel = Channel(Nil).new

      @sweep_job_running = false

      if @sweep_job_enabled && !(min_expire = {@max_idle_time_per_resource, @max_lifetime_per_resource}.reject(&.zero?).min?).nil?
        sweep_timer = ensure_time_span(pool_options.resource_sweeper_timer).as(Time::Span)

        if sweep_timer.zero?
          @sweep_timer = min_expire || sweep_timer
        end

        sweep_expired_job
      end
    end

    private macro ensure_time_span(value)
      if {{value}}.is_a? Number
        {{value}}.seconds
      else
        {{value}}
      end
    end

    # close all resources in the pool
    def close : Nil
      @total.each &.close
      @total.clear
      @idle.clear
      @resource_lifecycle.clear
      @sweep_job_close_channel.send(nil) if @sweep_job_running
    end

    record Stats,
      open_connections : Int32,
      idle_connections : Int32,
      in_flight_connections : Int32,
      max_connections : Int32,
      idle_expired_connections : Int64,
      lifetime_expired_connections : Int64

    # Returns stats of the pool
    def stats
      Stats.new(
        open_connections: @total.size,
        idle_connections: @idle.size,
        in_flight_connections: @inflight,
        max_connections: @max_pool_size,
        idle_expired_connections: @idle_expired_count,
        lifetime_expired_connections: @lifetime_expired_count
      )
    end

    def checkout : T
      res = sync do
        resource = nil

        until resource
          sweep_expired_job if !@sweep_job_running
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

        # Remove client if expired (either idle or lifetime)
        remove_expired!(resource)

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
          delete(resource)
        elsif can_increase_idle_pool
          # Checks lifetime expiration and updates last checked out time
          # Old idle expiration isn't checked because this replaces it.
          expire_info = @resource_lifecycle[resource]
          if lifetime_expired?(expire_info, Time.utc)
            resource.close
            delete(resource)
            ensure_minimum_fresh_resources
            return nil
          else
            expire_info.got_checked_out
          end

          @idle << resource
          if resource.responds_to?(:after_release)
            resource.after_release
          end
          idle_pushed = true
        else
          resource.close
          delete(resource)
        end
      end

      notify_availability if idle_pushed
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
        rescue e : PoolResourceExpired(T)
          # A `PoolResourceExpired` is raised internally at #checkout
          # and is both closed and deleted from the pool at the time of
          # raising. Although we can technically let the rescue of 
          # `PoolResourceLost(T)` handle the retry, we can avoid an expensive
          # mutex lock by doing so manually.
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
          @resource_lifecycle[resource].got_checked_out
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
      @resource_lifecycle.delete(resource)
    end

    # Inform availability_channel about an available resource
    private def notify_availability : Nil
      select
      when @availability_channel.send(nil)
      else
      end
    end

    # Checks if a resource has exceeded the maximum lifetime
    #
    # :nodoc:
    private def lifetime_expired?(time_entry : ResourceTimeEntry, time : Time = Time.utc)
      return false if @max_lifetime_per_resource.zero?

      expired = (time - time_entry.creation) >= @max_lifetime_per_resource
      @lifetime_expired_count += 1 if expired
      return expired
    end

    # Checks if a resource has exceeded the maximum idle time
    #
    # :nodoc:
    private def idle_expired?(time_entry : ResourceTimeEntry, time : Time = Time.utc)
      return false if @max_idle_time_per_resource.zero?

      expired = (time - time_entry.last_checked_out) >= @max_idle_time_per_resource
      @idle_expired_count += 1 if expired
      return expired
    end

    # Checks if the resource is expired. Deletes and raises `PoolResourceExpired` if so
    #
    # :nodoc:
    private def remove_expired!(resource : T)
      now = Time.utc
      expire_info = @resource_lifecycle[resource]

      expiration_type = if lifetime_expired?(expire_info, now)
                          PoolResourceLifetimeExpired
                        elsif idle_expired?(expire_info, now)
                          PoolResourceIdleExpired
                        else
                          nil
                        end

      if expiration_type
        resource.close
        delete(resource)
        ensure_minimum_fresh_resources
        raise expiration_type.new(resource)
      end
    end

    private def build_resource : T
      resource = @factory.call
      sync do
        @total << resource
        @idle << resource
        @resource_lifecycle[resource] = ResourceTimeEntry.new
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

    private def sweep_expired_job
      timer = @sweep_timer
      return if timer.nil? || (timer <= Time::Span::ZERO)

      @sweep_job_running = true

      spawn do
        loop do
          select
          when @sweep_job_close_channel.receive
            @sweep_job_running = false
            break
          when timeout(timer)
          end

          sync do
            now = Time.utc

            # Although not guaranteed, the first elements of @idle
            # should be the oldest
            @idle.each do |resource|
              expire_info = @resource_lifecycle[resource]

              if lifetime_expired?(expire_info, now) || idle_expired?(expire_info, now)
                resource.close
                delete(resource)
              end
            end

            ensure_minimum_fresh_resources

            # End job if there is no initial pool size and the entire pool has been expired
            break if !@initial_pool_size && @total.empty?
          end
        end
      end
    end

    # Ensure there are at least a minimum of @initial_pool_size non-expired resources
    #
    # Should be called after each expiration batch
    private def ensure_minimum_fresh_resources
      replenish = (@initial_pool_size - (@total.size + @inflight)).clamp(0, @initial_pool_size)

      return if replenish <= 0

      replenish.times do |index|
        begin
          @inflight += 1
          unsync do
            build_resource
            notify_availability
          end
        ensure
          @inflight -= 1
        end
      end

      return true
    end
  end
end
