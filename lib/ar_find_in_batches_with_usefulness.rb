require "ar_find_in_batches_with_usefulness/version"
require 'active_record/connection_adapters/postgresql_adapter'

module ArFindInBatchesWithUsefulness
  if defined? ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
    ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.class_eval do
      def readonly?(table = nil, column = nil)
        return @readonly unless @readonly.nil?
        @readonly = (select_value("SELECT pg_is_in_recovery();") == "t")
      end
    end
  end

  ActiveRecord::Relation.class_eval do
    def find_in_batches_with_usefulness(options = {}, &block)
      # already in a transaction (or transactions don't matter); cursor is fine
      if (connection.adapter_name == 'PostgreSQL' && (connection.readonly? || connection.open_transactions > (Rails.env.test? ? 1 : 0))) && !options[:start]
        self.activate { find_in_batches_with_cursor(options, &block) }
      elsif order_values.any? || group_values.any? || select_values.to_s =~ /DISTINCT/i || uniq_value || select_values.present? && !select_values.map(&:to_s).include?(primary_key)
        raise ArgumentError.new("GROUP and ORDER are incompatible with :start") if options[:start]
        self.activate { find_in_batches_with_temp_table(options, &block) }
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
          includes = includes_values
          klass.send(:with_exclusive_scope) do
            batch = connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
            while !batch.empty?
              ActiveRecord::Associations::Preloader.new(batch, includes).run if includes
              yield batch
              break if batch.size < batch_size
              batch = connection.uncached { klass.find_by_sql("FETCH FORWARD #{batch_size} FROM #{cursor}") }
            end
          end
          # not ensure; if the transaction rolls back due to another exception, it will
          # automatically close
          connection.execute("CLOSE #{cursor}")
        end
      end
    end

    def find_in_batches_with_temp_table(options = {})
      batch_size = options[:batch_size] || 1000
      sql = to_sql
      table = "#{table_name}_find_in_batches_temp_table_#{sql.hash.abs.to_s(36)}"
      table = table[-64..-1] if table.length > 64
      connection.execute "CREATE TEMPORARY TABLE #{table} AS #{sql}"
      begin
        index = "temp_primary_key"
        case connection.adapter_name
          when 'PostgreSQL'
            begin
              old_proc = connection.raw_connection.set_notice_processor {}
              connection.execute "ALTER TABLE #{table}
                             ADD temp_primary_key SERIAL PRIMARY KEY"
            ensure
              connection.raw_connection.set_notice_processor(&old_proc) if old_proc
            end
          when 'MySQL', 'Mysql2'
            connection.execute "ALTER TABLE #{table}
                             ADD temp_primary_key MEDIUMINT NOT NULL PRIMARY KEY AUTO_INCREMENT"
          when 'SQLite'
            # Sqlite always has an implicit primary key
            index = 'rowid'
          else
            raise "Temp tables not supported!"
        end

        includes = includes_values
        sql = "SELECT * FROM #{table} ORDER BY #{index} LIMIT #{batch_size}"
        klass.send(:with_exclusive_scope) do
          batch = klass.find_by_sql(sql)
          while !batch.empty?
            ActiveRecord::Associations::Preloader.new(batch, includes).run if includes
            yield batch
            break if batch.size < batch_size
            last_value = batch.last[index]

            sql = "SELECT *
             FROM #{table}
             WHERE #{index} > #{last_value}
             ORDER BY #{index} ASC
             LIMIT #{batch_size}"
            batch = klass.find_by_sql(sql)
          end
        end
      ensure
        temporary = "TEMPORARY " if connection.adapter_name == 'Mysql2'
        connection.execute "DROP #{temporary}TABLE #{table}"
      end
    end

  end
end


