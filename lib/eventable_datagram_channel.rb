class EventableDatagramChannel

  def initialize(socket_channel, binding, selector)
    @channel = socket_channel
    @binding = binding
    @selector = selector

    @close_scheduled = false
    @outbound_q = LinkedList.new

    @channel.register(@selector, SelectionKey::OP_READ, self)
  end

  def schedule_outbound_data(bb)
    return if @close_scheduled || bb.remaining < 1

    @outbound_q.addLast(Packet.new(bb, @return_address))
    @channel.register(@selector, SelectionKey::OP_WRITE | SelectionKey::OP_READ, self)
  end

  def schedule_outbound_datagram(bb, addr, port)
    return if @close_scheduled || bb.remaining < 1

    @outbound_q.addLast(Packet.new(bb, InetSocketAddress.new(addr, port)))
    @channel.register(@selector, SelectionKey::OP_WRITE | SelectionKey::OP_READ, self)
  end

  def schedule_close(after_writing)
    puts "NOT SCHEDULING CLOSE ON DATAGRAM"
    false
  end

  def close
    @channel.close
  end

  def read_inbound_data(bb)
    @return_address = @channel.receive(bb)
  end

  def write_outbound_data
    while !@outbound_q.isEmpty do
      packet = @outbound_q.getFirst

      written = 0

      begin
        written = @channel.write(packet.bb, packet.recipient)
      rescue IOException e
        return false
      end

      if written > 0 || packet.bb.remaining == 0
        @outbound_q.removeFirst
      else
        break
      end
    end

    @channel.register(@selector, SelectionKey::OP_READ, self) if @outbound_q.isEmpty

    return !(@close_scheduled && @outbound_q.isEmpty)
  end

  def comm_inactivity_timeout=(seconds)
    # todo
  end

  def get_peername
    return if @return_address.nil?
    [@return_address.getPort, @return_address.getHostname]
  end

  def get_sockname
    @socket ||= @channel.socket
    [@socket.getLocalPort, @socket.getLocalAddress.getHostAddress]
  end

  def watch_only
    false
  end

  def notify_readable
    false
  end

  def notify_writable
    false
  end

end
