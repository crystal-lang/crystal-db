module DB
  class ThreadLocalPool(T)
    @pools = Crystal::ThreadLocalValue(DB::Pool(T)).new

    def initialize(@pool_options : DB::Pool::Options = DB::Pool::Options.new, &@factory : -> T)
    end

    def close : Nil
    end

    def stats
      raise "not implemented"
    end

    def checkout : T
      pool.checkout
    end

    def checkout(&block : T ->)
      pool.checkout do |resource|
        yield resource
      end
    end

    def release(resource : T)
      pool.release(resource)
    end

    def retry
      pool.retry do
        yield
      end
    end

    def delete(resource : T)
      pool.delete(resource)
    end

    def each_resource
      each_pool do |p|
        p.each_resource do |conn|
          yield conn
        end
      end
    end

    def is_available?(resource : T)
      each_pool do |p|
        return true if p.is_available?(resource)
      end
      false
    end

    private def pool
      @pools.get do
        DB::Pool.new(@pool_options, &@factory)
      end
    end

    private def each_pool
      @pools.@mutex.sync do
        @pools.@values.each_value do |p|
          yield p
        end
      end
    end
  end
end
