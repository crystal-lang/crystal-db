module DB
  module BeginTransaction
    abstract def begin_transaction : Transaction

    def transaction
      tx = begin_transaction
      begin
        yield tx
      rescue e
        # TODO check if `unless tx.closed?` should be added.
        # If it is not added, when an exception occurs after the
        # transaction is closed (explicit rollback/commit)
        # the call to rollback will trigger an exception.
        # Since the code will be in between `#transaction(&block)`
        # seems reasonable to raise an error since it might trigger
        # a design flaw. Transaction might been commited already.
        # Maybe we should wrap e in another exception that clarifies
        # this scenario.
        tx.rollback
        raise e unless e.is_a?(DB::Rollback)
      else
        tx.commit unless tx.closed?
      end
    end
  end
end
