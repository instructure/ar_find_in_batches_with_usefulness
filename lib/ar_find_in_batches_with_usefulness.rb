require "ar_find_in_batches_with_usefulness/version"
require "active_record"

module ArFindInBatchesWithUsefulness
  ActiveRecord::Relation.class_eval do
    # based on https://github.com/afair/postgresql_cursor/blob/e8630d5f04e926a3fa152c78bc629d85c5cc573d/lib/postgresql_cursor/active_record/relation/cursor_iterators.rb
    # Returns sql string like #to_sql, but with bind parameters interpolated.
    # ActiveRecord sets up query as prepared statements with bind variables.
    # Cursors will prepare statements regardless.
    def to_unprepared_sql
      if connection.respond_to?(:unprepared_statement)
        connection.unprepared_statement do
          to_sql
        end
      else
        to_sql
      end
    end

    # Alias of find_in_batches that allows use of a cursor
    #
    # Only supports PostgreSQL adapter
    #
    # @param options include :cursor option to opt in
    #
    # @return enumerator unless block given, otherwise executes relation in batches
    def find_in_batches_with_usefulness(options = {}, &block)
      if connection.adapter_name == "PostgreSQL" && options[:cursor]
        find_in_batches_with_cursor(options, &block)
      else
        find_in_batches_without_usefulness(options) do |batch|
          yield batch
        end
      end
    end
    alias_method_chain :find_in_batches, :usefulness

    def find_in_batches_with_cursor(options = {})
      batch_size = options[:batch_size] || 1000
      unless block_given?
        return to_enum(:find_in_batches_with_cursor) do
          (size - 1).div(batch_size) + 1
        end
      end

      klass.transaction do
        begin
          sql = to_unprepared_sql
          cursor = "#{table_name}_in_batches_cursor_#{sql.hash.abs.to_s(36)}"
          connection.execute("DECLARE #{cursor} CURSOR FOR #{sql}")
          move_forward(cursor, options[:start]) if options[:start]
          batch = fetch_forward(batch_size, cursor)
          until batch.empty?
            yield batch
            break if batch.size < batch_size
            batch = fetch_forward(batch_size, cursor)
          end

          # not ensure; if the transaction rolls back due to another exception, it will
          # automatically close
          connection.execute("CLOSE #{cursor}")
          batch
        end
      end
    end

    private

    def move_forward(cursor, start)
      connection.execute("MOVE FORWARD #{start} IN #{cursor}")
    end

    def fetch_forward(batch_size, cursor)
      connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
    end
  end
end
