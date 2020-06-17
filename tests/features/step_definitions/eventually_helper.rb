module EventuallyHelper
  def self.eventually(timeout = 20, delay = 1)
    wait_until = Time.now + timeout
    begin
      yield
    rescue Exception => e
      raise e if Time.now >= wait_until
      sleep delay
      retry
    end
  end
end

