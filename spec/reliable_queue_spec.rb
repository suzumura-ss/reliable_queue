require 'lib/reliable_queue/queue_manager'

TEMPDIR = `mktemp -d`.strip unless defined? TEMPDIR
Kernel.at_exit{ FileUtils.rm_rf(TEMPDIR) }

def set_read_attribute(obj)
  class << obj
    attr_reader :logger, :queue, :reload_thread
    attr_reader :queue_dir, :waiting_dir, :retry_dir
    attr_reader :inmemory_object_limit, :reload_interval
  end
end

describe ReliableQueue::QueueManager do
  context "initialize" do
    it "should use default values" do
      rq = ReliableQueue::QueueManager.new
      set_read_attribute rq
      rq.logger.class.should == Logger
      rq.queue.class.should == Queue
      rq.reload_thread.class.should == Thread
      rq.queue_dir.should == "/tmp/reliable_queue"
      rq.waiting_dir.should == "/tmp/reliable_queue/waiting"
      rq.retry_dir.should == "/tmp/reliable_queue/retry"
      rq.inmemory_object_limit.should == 100
      rq.reload_interval.should == 10
      File.directory?(rq.queue_dir).should be_true
      File.directory?(rq.waiting_dir).should be_true
      File.directory?(rq.retry_dir).should be_true
      rq.stop
      FileUtils.rm_rf(rq.queue_dir)
    end

    it "should use specified values" do
      arg = {
        'queue_dir'=>TEMPDIR,
        'inmemory_object_limit'=>1000,
        'reload_interval'=>100
      }
      rq = ReliableQueue::QueueManager.new(arg)
      set_read_attribute rq
      rq.queue_dir.should == TEMPDIR
      rq.waiting_dir.should == "#{TEMPDIR}/waiting"
      rq.retry_dir.should == "#{TEMPDIR}/retry"
      rq.inmemory_object_limit.should == 1000
      rq.reload_interval.should == 100
      rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end

  context "push and pop" do
    before do
      @rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR
      set_read_attribute @rq
    end

    it "should success push and pop" do
      @rq << "hello"
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(1).items
      @rq.pop{|obj| obj.should == "hello"; true }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.queue_dir}/*"].should have(2).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items
    end

    it "should retry failed-pop." do
      @rq << "hello"
      @rq.pop{|obj| false }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(1).items
    end

    it "should retry raised-pop." do
      @rq << "hello"
      @rq.pop{|obj| raise RuntimeError }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(1).items
    end

    after do
      @rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end

  context "reload from master queue when start" do
    before do
      rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR
      (1..50).each{|v| rq << v }
      rq.stop
      rq = nil
      @rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR
      set_read_attribute @rq
    end

    it "should success retry" do
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(50).items
      (1..50).each{|v|
        @rq.pop{|o| o.should == v}
      }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items
    end

    after do
      @rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end

  context "reload from retry queue" do
    before do
      @rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR
      set_read_attribute @rq
    end

    it "should success retry" do
      @rq << "hello"
      @rq.pop{|obj| false }
      @rq.reload
      @rq.pop{|obj| obj.should == "hello"; true }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items
    end

    after do
      @rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end

  context "reload from waiting queue" do
    before do
      @rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR, 'inmemory_object_limit'=>1
      set_read_attribute @rq
    end

    it "should success refill queue" do
      @rq << "1"
      @rq << "2"
      @rq << "3"
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(1).items
      Dir["#{@rq.waiting_dir}/*"].should have(2).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items

      @rq.pop{|obj| obj.should == "1"; true }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(2).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items

      @rq.reload
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(1).items
      Dir["#{@rq.waiting_dir}/*"].should have(1).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items

      @rq.pop{|obj| obj.should == "2"; true }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(1).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items

      @rq.reload
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(1).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items

      @rq.pop{|obj| obj.should == "3"; true }
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items
    end

    after do
      @rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end

  context "random test" do
    before do
      @rq = ReliableQueue::QueueManager.new 'queue_dir'=>TEMPDIR, 'reload_interval'=>1.0
      set_read_attribute @rq
      @th = (0..9).map{|t|
              Thread.new{ (1..500).each{|v|
                @rq << [t, v*10+t]
                sleep 0.01
              }}
            }
    end

    it "should success all" do
      q = Queue.new
      Thread.new{
        while q.size<10*500; @rq.pop{|v| q << v }; end 
      }.join
      while q.size>0
        t, v = q.pop
        t.should == (v%10)
        v /= 10
        v.should >=1
        v.should <=500
      end
      @rq.queue.size.should == 0
      Dir["#{@rq.queue_dir}/[0-9]*"].should have(0).items
      Dir["#{@rq.waiting_dir}/*"].should have(0).items
      Dir["#{@rq.retry_dir}/*"].should have(0).items
    end

    after do
      @rq.stop
      FileUtils.rm_rf(TEMPDIR)
    end
  end
end
