require "logger"

module DB
  # :nodoc:
  class Logger
    @logger : Proc(String, Void)?
    @enabled = false
    @default_logger = ::Logger.new(STDOUT)

    def config(@logger)
    end

    def logging=(@enabled : Bool)
    end

    def log(query : String, *args)
      return if !@enabled
      if args && args.size > 0
        log = %{QUERY "#{query}" WITH PARAMS: #{args.join(", ")}}
      else
        log = %{QUERY "#{query}"}
      end
      if (logger = @logger)
        logger.call(log)
      else
        @default_logger.info(log)
      end
    end
  end
end
