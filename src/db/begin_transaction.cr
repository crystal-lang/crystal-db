module DB
  module BeginTransaction
    abstract def begin_transaction : Transaction

    def transaction
      tx = begin_transaction
      begin
        yield tx
      rescue e
        tx.rollback unless tx.closed?
        raise e unless e.is_a?(DB::Rollback)
      else
        tx.commit unless tx.closed?
      end
    end
  end
end
