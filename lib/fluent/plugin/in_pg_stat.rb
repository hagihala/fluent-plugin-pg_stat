require 'fluent/input'
require 'json'
require 'pg'

module Fluent
  class PgStatStatementsInput < Input
    Plugin.register_input('pg_stat_statements', self)

    class << self
      unless method_defined?(:desc)
        def desc(description)
        end
      end
    end

    desc 'PostgreSQL host'
    config_param :host, :string
    desc 'RDBMS port (default: 5432)'
    config_param :port, :integer, default: 5432
    desc 'login user name'
    config_param :username, :string, default: nil
    desc 'login password'
    config_param :password, :string, default: nil, secret: true
    desc 'past to store last value'
    config_param :state_file, :string, default: nil
    desc 'tag'
    config_param :tag, :string, default: nil
    desc 'interval in second to run SQLs (optional)'
    config_param :select_interval, :time, default: 60

    define_method(:log) { $log } unless method_defined?(:log)

    def configure(conf)
      super

      @column_names = %w(
      calls total_time rows
      shared_blks_hit shared_blks_read shared_blks_dirtied shared_blks_written
      local_blks_hit local_blks_read local_blks_dirtied local_blks_written
      temp_blks_read temp_blks_written
      blk_read_time blk_write_time
      )
    end

    def start
      @conn = PG.connect(
        host: @host,
        dbname: 'postgres',
        user: @username,
        password: @password
      )
      @conn.type_map_for_results = PG::BasicTypeMapForResults.new @conn

      @stop_flag = false
      @thread = Thread.new(&method(:thread_main))
    end

    def shutdown
      @stop_flag = true
      $log.debug 'Waiting for thread to finish'
      @thread.join
    end

    def thread_main
      until @stop_flag
        sleep 60

        begin
          me = MultiEventStream.new
          begin
            old_metrics = JSON.parse(IO.read(@state_file))
          rescue => e
            $log.info e
            old_metrics = {}
          end
          new_metrics = {}

          # More than one records of the same query string can exist
          # because queries longer than xxx bbytes are truncated.
          # Treat them as identical.
          now = Engine.now
          @conn.exec(<<-EOS
          select query, #{@column_names.map{|col| "cast (sum(#{col}) as double precision) as #{col}"}.join(', ')} from pg_stat_statements group by query order by total_time desc
          EOS
                    ).each do |row|
            new_metrics['timestamp'] = now.to_f
            query = row['query']
            new_metrics[query] = {}

            @column_names.each do |col|
              new_metrics[query][col] = row[col]
              row[col] = if !old_metrics[query].nil? && old_metrics[query][col].is_a?(Numeric)
                           val = (row[col] - old_metrics[query][col]) * 60 / (new_metrics['timestamp'] - old_metrics['timestamp'])
                           val >= 0 ? val : nil
                         end
            end

            # Calculate average values
            if row['calls'].is_a?(Numeric) && row['calls'] != 0
              row['avg_rows'] = row['rows'] / row['calls'] if row['rows'].is_a? Numeric
              row['avg_time'] = row['total_time'] / row['calls'] if row['total_time'].is_a? Numeric
            end

            me.add(now, row)
          end
          @router.emit_stream(@tag, me)
          IO.write(@state_file, new_metrics.to_json)

        rescue => e
          log.error 'unexpected error', error: e.message, error_class: e.class
          log.error_backtrace e.backtrace
        end
      end
    end
  end
end
