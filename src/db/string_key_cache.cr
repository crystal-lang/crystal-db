module DB
  class StringKeyCache(T)
    @cache = {} of String => T
    @mutex = Mutex.new

    def fetch(key : String) : T
      @mutex.synchronize do
        value = @cache.fetch(key, nil)
        value = @cache[key] = yield unless value
        value
      end
    end

    def each_value
      @mutex.synchronize do
        @cache.each do |_, value|
          yield value
        end
      end
    end

    def clear
      @mutex.synchronize do
        @cache.clear
      end
    end
  end
end
