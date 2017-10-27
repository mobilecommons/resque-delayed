module Resque
  class << self
    def enqueue_at(queue, time, *args)
      Resque::Delayed.create_at(queue, time, *args)
    end

    def enqueue_in(queue, offset, *args)
      Resque::Delayed.create_in(queue, offset, *args)
    end
  end
end
