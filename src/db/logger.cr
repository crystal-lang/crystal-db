module DB
  # :nodoc:
  class Logger
    @@logger : Proc(String, Void)?
    @@enabled = false

    def self.config(@@logger)
    end

    def self.logging=(@@enabled : Bool)
    end

    def self.log(query : String)
      if (logger = @@logger) && @@enabled
        logger.call(query)
      end
    end
  end
end
