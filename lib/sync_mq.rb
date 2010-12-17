require 'mq'

class SyncMQ < MQ
  Dir[File.expand_path(File.join(File.dirname(__FILE__), 'sync_mq', '*.rb'))].each do |file|
    require file
  end

  @@thread = nil
  SPIN_TIMEOUT = 5

  class << self
    def start_reactor
      unless EM.reactor_running?
        if @@thread.nil? || !@@thread.alive?
          current = Thread.current
          @@thread = Thread.new do 
            EM.run do
              EM.next_tick(current.wakeup)
            end
          end 
          Thread.stop
        end
      end
    end
  end
  
  def initialize(connection = nil)
    self.class.start_reactor
    super(connection)
    spin("waiting to establish a connection") { self.connected }
  end

  def rpc(name, obj = nil)
    blocking_rpcs[name] ||= RPC.new(self, name, obj)
  end

  def close
    super
    spin("waiting to close the connection") { !self.connected }
  end

  def blocking_rpcs
    @blocking_rpcs ||= {}
  end

  private
    def spin(description)
      start_time = Time.now.to_f
      until yield() == true
        sleep(0.1)
        raise "SyncMQ::Timed out: #{description}" if Time.now.to_f - start_time > SPIN_TIMEOUT
      end 
    end
end
