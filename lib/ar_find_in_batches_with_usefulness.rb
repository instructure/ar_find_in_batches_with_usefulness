require "ar_find_in_batches_with_usefulness/version"

module ArFindInBatchesWithUsefulness
  ActiveRecord::Relation.class_eval do
    def find_in_batches_with_usefulness(options = {}, &block)
      if (connection.adapter_name == 'PostgreSQL' && !options[:start])
        find_in_batches_with_cursor(options, &block)
      else
        find_in_batches_without_usefulness(options) do |batch|
          klass.send(:with_exclusive_scope) { yield batch }
        end
      end
    end
    alias_method_chain :find_in_batches, :usefulness

    def find_in_batches_with_cursor(options = {}, &block)
      batch_size = options[:batch_size] || 1000
      klass.transaction do
        begin
          sql = to_sql
          cursor = "#{table_name}_in_batches_cursor_#{sql.hash.abs.to_s(36)}"
          connection.execute("DECLARE #{cursor} CURSOR FOR #{sql}")
          batch = connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
          while !batch.empty?
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


