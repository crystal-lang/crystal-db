require "weak_ref"

module DB
  class Pool(T)
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
    # signal how many existing connections are waited for
    @waiting_resource : Int32
    # global pool mutex
    @mutex : Mutex

    def initialize(@initial_pool_size = 1, @max_pool_size = 0, @max_idle_pool_size = 1, @checkout_timeout = 5.0,
                   @retry_attempts = 1, @retry_delay = 0.2, &@factory : -> T)
      @availability_channel = Channel(Nil).new
      @waiting_resource = 0
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

    record Stats, open_connections : Int32

    # Returns stats of the pool
    def stats
      Stats.new(
        open_connections: @total.size
      )
    end

    def checkout : T
      res = sync do
        resource = nil

        until resource
          resource = if @idle.empty?
                       if can_increase_pool?
                         @inflight += 1
                         r = unsync { build_resource }
                         @inflight -= 1
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

      res.before_checkout
      res
    end

    # ```
    # selected, is_candidate = pool.checkout_some(candidates)
    # ```
    # `selected` be a resource from the `candidates` list and `is_candidate` == `true`
    # or `selected` will be a new resource and `is_candidate` == `false`
    def checkout_some(candidates : Enumerable(WeakRef(T))) : {T, Bool}
      sync do
        candidates.each do |ref|
          resource = ref.value
          if resource && is_available?(resource)
            @idle.delete resource
            resource.before_checkout
            return {resource, true}
          end
        end
      end

      resource = checkout
      {resource, candidates.any? { |ref| ref.value == resource }}
    end

    def release(resource : T) : Nil
      idle_pushed = false

      sync do
        if can_increase_idle_pool
          @idle << resource
          resource.after_release
          idle_pushed = true
        else
          resource.close
          @total.delete(resource)
        end
      end

      if idle_pushed && are_waiting_for_resource?
        @availability_channel.send nil
      end
    end

    # :nodoc:
    # Will retry the block if a `ConnectionLost` exception is thrown.
    # It will try to reuse all of the available connection right away,
    # but if a new connection is needed there is a `retry_delay` seconds delay.
    def retry
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
        rescue e : ConnectionLost
          # if the connection is lost close it to release resources
          # and remove it from the known pool.
          sync { delete(e.connection) }
          e.connection.close
        rescue e : ConnectionRefused
          # a ConnectionRefused means a new connection
          # was intended to be created
          # nothing to due but to retry soon
        end
      end
      raise PoolRetryAttemptsExceeded.new
    end

    # :nodoc:
    def each_resource
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
      @total << resource
      @idle << resource
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

    {% if compare_versions(Crystal::VERSION, "0.34.0-0") > 0 %}
      private def wait_for_available
        sync_inc_waiting_resource

        select
        when @availability_channel.receive
          sync_dec_waiting_resource
        when timeout(@checkout_timeout.seconds)
          sync_dec_waiting_resource
          raise DB::PoolTimeout.new
        end
      end
    {% else %}
      private def wait_for_available
        timeout = TimeoutHelper.new(@checkout_timeout.to_f64)
        sync_inc_waiting_resource

        timeout.start

        index, _ = Channel.select(@availability_channel.receive_select_action, timeout.receive_select_action)
        case index
        when 0
          timeout.cancel
          sync_dec_waiting_resource
        when 1
          sync_dec_waiting_resource
          raise DB::PoolTimeout.new
        else
          raise DB::Error.new
        end
      end
    {% end %}

    private def sync_inc_waiting_resource
      sync { @waiting_resource += 1 }
    end

    private def sync_dec_waiting_resource
      sync { @waiting_resource -= 1 }
    end

    private def are_waiting_for_resource?
      @waiting_resource > 0
    end

    private def sync
      @mutex.lock
      begin
        yield
      ensure
        @mutex.unlock
      end
    end

    private def unsync
      @mutex.unlock
      begin
        yield
      ensure
        @mutex.lock
      end
    end

    class TimeoutHelper
      def initialize(@timeout : Float64)
        @abort_timeout = false
        @timeout_channel = Channel(Nil).new
      end

      def receive_select_action
        @timeout_channel.receive_select_action
      end

      def start
        spawn do
          sleep @timeout
          unless @abort_timeout
            @timeout_channel.send nil
          end
        end
      end

      def cancel
        @abort_timeout = true
      end
    end
  end
end
