require "ar_find_in_batches_with_usefulness/version"
require "active_record"

module ArFindInBatchesWithUsefulness
  ActiveRecord::Relation.class_eval do
    def find_in_batches_with_cursor(options = {})
      batch_size = options[:batch_size] || 1000
      klass.transaction do
        begin
          sql = to_sql
          cursor = "#{table_name}_in_batches_cursor_#{sql.hash.abs.to_s(36)}"
          connection.execute("DECLARE #{cursor} CURSOR FOR #{sql}")
          batch = connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
          until batch.empty?
            yield batch
            break if batch.size < batch_size
            batch = connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
          end

          # not ensure; if the transaction rolls back due to another exception, it will
          # automatically close
          connection.execute("CLOSE #{cursor}")
          batch
        end
      end
    end
  end
end
