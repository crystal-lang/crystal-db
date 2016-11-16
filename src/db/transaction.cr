module DB
  abstract class Transaction
    include Disposable

    abstract def connection : Connection

    def commit
      close!
    end

    def rollback
      close!
    end

    private def close!
      raise DB::Error.new("Transaction already closed") if closed?
      close
    end
  end

  class TopLevelTransaction < Transaction
    # :nodoc:
    getter connection

    def initialize(@connection : Connection)
      @connection.perform_begin_transaction
    end

    def commit
      @connection.perform_commit_transaction
      close!
    end

    def rollback
      @connection.perform_rollback_transaction
      close!
    end

    protected def do_close
      connection.release_from_transaction
    end
  end
end
