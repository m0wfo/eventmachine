class EventableSocketChannel

  attr_reader :binding, :channel, :watch_only, :notify_readable, :notify_writable
  attr_accessor :attached
  
  def initialize(socket_channel, binding, selector)
    @channel = socket_channel
    @binding = binding
    @selector = selector

    @close_scheduled = false
    @connect_pending = false
    @watch_only = false
    @attached = false
    @notify_readable = false
    @notify_writable = false

    @outbound_q = LinkedList.new
  end

  def register
    @channel_key ||= @channel.register(@selector, current_events, self)
  end

  def close
    if @channel_key.nil?
      @channel_key.cancel
      @chennal_key = nil
    end

    if @attached
      begin
        f = @channel.getClass.getDeclaredField("fd")
        f.setAccessible(true)
        fd = f.get(@channel)

        f = fd.getClass.getDeclaredField("fd")
        f.setAccessible(true)
        f.set(fd, -1)

        return
      rescue NoSuchFieldException e
      rescue IllegalAccessException e
      end
    end

    @channel.close
  end

  def cleanup
    if @attached
      f = @channel.getClass.getDeclaredField("fdVal")
      f.setAccessible(true)
      f.set(@channel, -1)
    end

    @channel = nil
  end

  def schedule_outbound_data(bb)
    return if @close_scheduled || bb.remaining < 1

    # todo: implement SSL/TLS

    @outbound_q.addLast(bb)
    update_events
  end

  def read_inbound_data(bb)
    raise "eof" if @channel.read(bb) == -1
  end

  def write_outbound_data
    while !@outbound_q.isEmpty do
      b = @outbound_q.getFirst

      @channel.write(b) if b.remaining > 0

      if b.remaining == 0
        @outbound_q.removeFirst
      else
        break
      end
    end

    update_events if @outbound_q.isEmpty && !@close_scheduled

    return !(@close_scheduled && @outbound_q.isEmpty)
  end

    def connect_pending=(is_pending)
      @connect_pending = is_pending
      update_events if is_pending
    end

    def finish_connecting
      @channel.finishConnect

      @connect_pending = false
      update_events
      return true
    end
  end

    def schedule_close
      @outbound_q.clear if !@after_writing

      return true if @outbound_q.isEmpty

      update_events
      @close_scheduled = true
      return false
    end

    def start_tls
      # todo
    end

    def comm_inactivity_timeout=(seconds)
      # todo
    end

    def get_peername
      @socket ||= @channel.socket
      [@socket.getPort, @socket.getInetAddress.getHostAddress]
    end

    def get_sockname
      @socket ||= @channel.socket
      [@socket.getLocalPort, @socket.getLocalAddress.getHostAddress]
    end

    def watch_only=(only)
      @watch_only = only
      update_events if only
    end

    def notify_readable=(readable)
      @notify_readable = readable
      update_events if readable
    end

    def notify_writable=(writable)
      @notify_writable = writable
      update_events if writable
    end

    def update_events
      return if @channel_key.nil?
      events = current_events
      @channel_key.interestOps(events) if @channel_key.interestOps != events
    end

    def current_events
      events = 0

      if @watch_only
        events += SelectionKey::OP_READ if @notify_readable
        events += SelectionKey::OP_WRITE if @notify_writable
      else
        if @connect_pending
          events += SelectionKey::OP_CONNECT
        else
          events += SelectionKey::OP_READ
          events += SelectionKey::OP_WRITE if !@outbound_q.isEmpty
        end
      end

      events
    end
  
end
