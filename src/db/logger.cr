require "logger"

module DB
  # :nodoc:
  class Logger
    @logger : Proc(String, Void) | ::Logger = ::Logger.new(STDOUT)
    @enabled = false

    def config(@logger = ::Logger.new(STDOUT))
    end

    def logging=(@enabled : Bool)
    end

    def log(query : String, *args)
      return if !@enabled
      logger = @logger
      log = args && args.size > 0 ? %{QUERY "#{query}" with params: #{args.join(", ")}} : %{QUERY "#{query}"}
      logger.is_a?(::Logger) ? logger.info(log) : logger.call(log)
    end
  end
end
