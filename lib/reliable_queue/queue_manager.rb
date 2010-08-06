require 'fileutils'
require 'thread'
require 'logger'
require 'json'
require 'uuid'

module ReliableQueue

class QueueManager
  def initialize(arg={})
    begin
      UUID.generate
    rescue NameError
      class << UUID
        def generate; UUID.new; end
      end
    end
    DEFAULT_ARGS.merge(arg).each {|k,v|
      self.instance_variable_set("@#{k}", v)
    }
    @logger ||= ::Logger.new(STDOUT)
    @waiting_dir = File.join(@queue_dir, 'waiting')
    @retry_dir = File.join(@queue_dir, 'retry')
    @queue = Queue.new
    
    FileUtils.mkdir_p([@queue_dir, @waiting_dir, @retry_dir])
    reload_from_dir(@queue_dir)
    @reload_thread = Thread.new{ reloader }
  end

  def stop
    @reload_thread[:stop] = true
    @reload_thread.raise StopReloader
    @reload_thread.join
  end

  def reload
    reload_once
  end

  def push(obj)
    if @queue.size < @inmemory_object_limit
      k = push_to_file(obj, @queue_dir)
      @queue << [k, obj]
    else
      push_to_file(obj, @waiting_dir)
    end
  end
  alias :<< :push

  def pop
    raise ArgumentError, "No block given." unless block_given?
    k, obj = @queue.pop
    k = File.join(@queue_dir, k)
    if (yield obj rescue nil)
      FileUtils.rm_f(k)
    else
      begin
        FileUtils.mv(k, @retry_dir)
      rescue => e
        Logger.error { "#{self.class.name}: retry failed - #{e}" }
      end
    end
  end

private
  class StopReloader < RuntimeError; end
  FILE_MODE = File::WRONLY|File::CREAT|File::TRUNC|File::EXCL
  DEFAULT_ARGS = {
    'queue_dir' => '/tmp/reliable_queue',
    'inmemory_object_limit' => 100,
    'reload_interval' => 10,
  }.freeze

  def reload_from_dir(q_dir)
    Dir[File.join(q_dir, '*')].sort.each{|p|
      if @queue.size<@inmemory_object_limit
        begin
          obj = Marshal.load(IO.read(p))
          FileUtils.mv(p, @queue_dir) if q_dir!=@queue_dir
          @queue << [File.basename(p), obj]
        rescue TypeError, Errno::EACCES => e
          @logger.error { "#{self.class.name}: reload(#{q_dir}) failed: #{p} - #{e}" }
        rescue Errno::ENOENT
        end
      elsif q_dir == @queue_dir
        # 多すぎるのでスキマ送り
        begin
          FileUtils.mv(p, @waiting_dir)
        rescue Errno::EACCES => e
          @logger.error { "#{self.class.name}: reload(#{q_dir} failed: #{p} - #{e}" }
        rescue Errno::ENOENT
        end
      end
    }
  rescue NameError => e; raise e
  rescue
  end

  def reloader
    until Thread.current[:stop]
      begin
        sleep @reload_interval
      rescue StopReloader
        break if Thread.current[:stop]
      end
      reload_once
    end
  end

  def reload_once
    reload_from_dir(@waiting_dir) if @queue.size < @inmemory_object_limit
    reload_from_dir(@retry_dir) if @queue.size < @inmemory_object_limit
  rescue NameError => e; raise e
  rescue StopReloader
    retry
  rescue
  end

  def push_to_file(obj, q_dir)
    k = "#{Time.now.strftime('%Y%m%dT%H%M%S')}.#{UUID.generate}"
    File.open(File.join(q_dir, k), FILE_MODE) {|f|
      f.write Marshal.dump(obj)
    }
    k
  end
end

end
# vim: filetype=ruby sw=2 sts=2 ts=4 expandtab :
