module DB
  # :nodoc:
  struct EnumerableConcat(S, T, U)
    include Enumerable(S)

    def initialize(@e1 : T, @e2 : U)
    end

    def each
      if e1 = @e1
        @e1.each do |e|
          yield e
        end
      end
      if e2 = @e2
        e2.each do |e|
          yield e
        end
      end
    end

    # returns given `e1 : T` an `Enumerable(T')` and `e2 : U` an `Enumerable(U') | Nil`
    # it returns an `Enumerable(T' | U')` that enumerates the elements of `e1`
    # and, later, the elements of `e2`.
    def self.build(e1 : T, e2 : U)
      return e1 if e2.nil? || e2.empty?
      return e2 if e1.nil? || e1.empty?
      EnumerableConcat(Union(typeof(sample(e1)), typeof(sample(e2))), T, U).new(e1, e2)
    end

    private def self.sample(c : Enumerable?)
      c.not_nil!.each do |e|
        return e
      end
      raise ""
    end
  end
end
