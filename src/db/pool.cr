module DB
  class Pool(T)
    @initial_pool_size : Int32
    # maximum amount of objects in the pool. Either available or in use.
    @max_pool_size : Int32
    @available = Set(T).new
    @total = [] of T
    @checkout_timeout : Float64

    def initialize(@initial_pool_size = 1, @max_pool_size = 1, @max_idle_pool_size = 1, @checkout_timeout = 5.0, &@factory : -> T)
      @initial_pool_size.times { build_resource }

      @availability_channel = Channel(Nil).new
      @waiting_resource = 0
      @mutex = Mutex.new
    end

    # close all resources in the pool
    def close : Nil
      @total.each &.close
    end

    def checkout : T
      resource = if @available.empty?
                   if can_increase_pool
                     build_resource
                   else
                     wait_for_available
                     pick_available
                   end
                 else
                   pick_available
                 end

      @available.delete resource
      resource
    end

    def release(resource : T) : Nil
      if can_increase_idle_pool
        @available << resource
        @availability_channel.send nil if are_waiting_for_resource?
      else
        resource.close
        @total.delete(resource)
      end
    end

    private def build_resource : T
      resource = @factory.call
      @total << resource
      @available << resource
      resource
    end

    private def can_increase_pool
      @total.size < @max_pool_size
    end

    private def can_increase_idle_pool
      @available.size < @max_idle_pool_size
    end

    private def pick_available
      @available.first
    end

    private def wait_for_available
      timeout = TimeoutHelper.new(@checkout_timeout.to_f64, ->{ @availability_channel.send nil })
      inc_waiting_resource

      timeout.start
      # if there are no available resources, sleep until one is available
      @availability_channel.receive
      if timeout.timeout_reached?
        dec_waiting_resource
        raise DB::PoolTimeout.new
      end

      # double check there is something available to be checkedout
      while @available.empty?
        @availability_channel.receive
        if timeout.timeout_reached?
          dec_waiting_resource
          raise DB::PoolTimeout.new
        end
      end

      timeout.cancel
      dec_waiting_resource
    end

    private def inc_waiting_resource
      @mutex.synchronize do
        @waiting_resource += 1
      end
    end

    private def dec_waiting_resource
      @mutex.synchronize do
        @waiting_resource -= 1
      end
    end

    private def are_waiting_for_resource?
      @mutex.synchronize do
        @waiting_resource > 0
      end
    end

    class TimeoutHelper
      def initialize(@timeout : Float64, @tick : Proc(Nil))
        @abort_timeout = false
        @should_timeout = false
      end

      def start
        spawn do
          sleep @timeout
          unless @abort_timeout
            @should_timeout = true
            @tick.call
          end
        end
      end

      def cancel
        @abort_timeout = true
      end

      def timeout_reached?
        @should_timeout
      end
    end
  end
end
