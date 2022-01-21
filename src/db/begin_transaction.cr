module DB
  module BeginTransaction
    # Creates a transaction from the current context.
    # If is expected that either `Transaction#commit` or `Transaction#rollback`
    # are called explicitly to release the context.
    abstract def begin_transaction : Transaction

    # yields a transaction from the current context.
    # Query the database through `Transaction#connection` object.
    # If an exception is thrown within the block a rollback is performed.
    # The exception thrown is bubbled unless it is a `DB::Rollback`.
    # From the yielded object `Transaction#commit` or `Transaction#rollback`
    # can be called explicitly.
    # Returns the value of the block when no exception was raised.
    def transaction(& : Transaction -> T) : T? forall T
      tx = begin_transaction
      begin
        res = yield tx
      rescue DB::Rollback
        tx.rollback unless tx.closed?
        res
      rescue e
        unless tx.closed?
          # Ignore error in rollback.
          # It would only be a secondary error to the original one, caused by
          # corrupted connection state.
          tx.rollback rescue nil
        end
        raise e
      else
        tx.commit unless tx.closed?
        res
      end
    end
  end
end
