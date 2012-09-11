java_import java.util.TreeMap
java_import java.util.HashMap
java_import java.util.ArrayList
java_import java.util.concurrent.atomic.AtomicBoolean
java_import java.nio.ByteBuffer
java_import java.nio.channels.ClosedChannelException
java_import java.nio.channels.SelectionKey
java_import java.nio.channels.Selector
java_import java.nio.channels.ServerSocketChannel
java_import java.io.IOException
java_import java.net.InetSocketAddress

class EmReactor
  
  EM_TIMER_FIRED = 100
  EM_CONNECTION_READ = 101
  EM_CONNECTION_UNBOUND = 102
  EM_CONNECTION_ACCEPTED = 103
  EM_CONNECTION_COMPLETED = 104
  EM_LOOPBREAK_SIGNAL = 105
  EM_CONNECTION_NOTIFY_READABLE = 106
  EM_CONNECTION_NOTIFY_WRITABLE = 107
  EM_SSL_HANDSHAKE_COMPLETED = 108
  EM_SSL_VERIFY = 109
  EM_PROXY_TARGET_UNBOUND = 110
  EM_PROXY_COMPLETED = 111

  def initialize
    @timers = TreeMap.new
    @connections = HashMap.new
    @acceptors = HashMap.new
    @new_connections = ArrayList.new
    @unbound_connections = ArrayList.new
    @detached_connections = ArrayList.new

    @binding_index = 0
    @loop_breaker = AtomicBoolean.new(false)

    @my_read_buffer = ByteBuffer.allocate(32*1024)
    @timer_quantum = 98
  end

  def event_callback(sig, event_type, data, data2=nil)
    puts "Default callback: #{sig}#{event_type}#{data}#{data2}"
  end

  def run
    begin
      @my_selector = Selector.open()
      @run_reactor = true
    rescue IOException => e
      raise "Could not open selector"
    end

    while @run_reactor
      run_loop_breaks
      break if !@run_reactor

      run_timers
      break if !@run_reactor

      remove_unbound_connections
      check_io
      add_new_connections
      process_io
    end

    close
  end

  def add_new_connections
    @detached_connections.each do |ec|
      ec.cleanup()
    end
    @detached_connections.clear()

    @new_connections.each do |b|
      ec = @connections.get(b)
      if !ec.nil?
        begin
          ec.register
        rescue ClosedChannelException => e
          @unbound_connections.add(ec.getBinding())
        end
      end
    end
    @new_connections.clear()
  end

  def remove_unbound_connections
    @unbound_connections.each do |b|
      ec = @connections.remove(b)

      if !ec.nil?
        event_callback(b, EM_CONNECTION_UNBOUND, nil)
        ec.close

        if !ec.nil? && ec.attached
          @detached_connections.add(ec)
        end
      end
    end
    @unbound_connections.clear()
  end

  def check_io
    timeout = 0

    if @new_connections.size() > 0
      timeout = -1
    elsif !@timers.isEmpty()
      now = java.util.Date.new.getTime()
      k = @timers.firstKey()
      diff = k-now

      if diff <= 0
        timeout = -1
      else
        timeout = diff
      end
    else
      timeout = 0
    end

    begin
      if timeout == -1
        @my_selector.selectNow()
      else
        @my_selector.select(timeout)
      end
    rescue IOException => e
      e.printStackTrace  
    end
  end

  def process_io
    it = @my_selector.selectedKeys().iterator()
    while it.hasNext() do
      k = it.next()
      it.remove()

      if k.isConnectable()
        is_connectable(k)
      elsif k.isAcceptable()
        is_acceptable(k)
      else
        if k.isWritable()
          is_writable(k)
        else k.isReadable()
          is_readable(k)
        end
      end
    end
  end

  def is_acceptable(key)
    ss = key.channel

    10.times do
      begin
        sn = ss.accept
        break if sn.nil?
      rescue IOException => e
        e.printStackTrace()
        key.cancel()

        server = @acceptors.remove(key.attachment)
        server.close if !server.nil?
        break
      end

      begin
        sn.configureBlocking(false)
      rescue IOException => e
        e.printStackTrace()
        next
      end

      b = create_binding
      ec = EventableSocketChannel.new(sn, b, @my_selector)
      @connections.put(b, ec)
      @new_connections.add(b)

      event_callback(key.attachment, EM_CONNECTION_ACCEPTED, nil, b)
    end
  end

  def is_readable(key)
    ec = key.attachment
    b = ec.binding

    if ec.watch_only
      event_callback(b, EM_CONNECTION_READ, @my_read_buffer) if ec.notify_readable
    else
      @my_read_buffer.clear

      begin
        ec.read_inbound_data(@my_read_buffer)
        @my_read_buffer.flip
        event_callback(b, EM_CONNECTION_READ, @my_read_buffer)
      rescue IOException => e
        @unbound_connections.add(b)
      end
    end
  end

  def is_writable(key)
    ec = key.attachment()
    b = ec.binding

    if ec.watch_only
      event_callback(b, EM_CONNECTION_NOTIFY_WRITABLE, nil) if ec.notify_writable
    else
      begin
        @unbound_connections.add(b) if !ec.write_outbound_data
      rescue IOException => e
        @unbound_connections.add(b)
      end
    end
  end

  def is_connectable(key)
    ec = key.attachment
    b = ec.binding

    begin
      if ec.finish_connecting
        event_callback(b, EM_CONNECTION_COMPLETED, nil)
      else
        @unbound_connections.add(b)
      end
    rescue IOException => e
      @unbound_connections.add(b)
    end
  end

  def close
    @my_selector.close if !@my_selector.nil?

    @acceptors.each {|a| a.close }

    conns = ArrayList.new

    @connections.each do |ec|
      conns.add(ec) if !ec.nil?
    end
    @connections.clear

    conns.each do |ec|
      event_callback(ec.get_binding, EM_CONNECTION_UNBOUND, nil)
      ec.close
      @detached_connections.add(ec) if sc.is_attached
    end

    @detached_connections.each { |ec| ec.cleanup }
    @detached_connections.clear()
  end

  def run_loop_breaks
    event_callback(0, EM_LOOPBREAK_SIGNAL, nil) if @loop_breaker.getAndSet(false)
  end

  def stop
    @run_reactor = false
    signal_loop_break    
  end

  def run_timers
    now = java.util.Date.new.getTime

    while !@timers.isEmpty
      k = @timers.firstKey
      break if k > now

      callbacks = @timers.get(k)
      @timers.remove(k)

      callbacks.each { |cb| event_callback(0, EM_TIMER_FIRED, nil, cb) }
    end
  end

  def install_oneshot_timer(milliseconds)
    s = create_binding
    deadline = java.util.Date.new.getTime + milliseconds

    if @timers.containsKey(deadline)
      @timers.get(deadline).add(s)
    else
      callbacks = ArrayList.new
      callbacks.add(s)
      @timers.put(deadline, callbacks)
    end

    s
  end

  def start_tcp_server(addr, port=nil)
    if !port.nil?
      addr = InetSocketAddress.new(addr, port)
    end

    begin
      server = ServerSocketChannel.open
      server.configureBlocking(false)
      server.socket.bind(addr)
      s = create_binding
      @acceptors.put(s, server)
      server.register(@my_selector, SelectionKey::OP_ACCEPT, s)
      return s
    rescue IOException => e
      raise err
    end
  end

  def stop_tcp_server(sig)
    server = @acceptors.remove(sig)
    if !server.nil?
      server.close
    else
      raise "err"
    end
  end

  def open_udp_socket(addr, port=nil)
    if port.nil?
      addr = InetSocketAddress.new(addr, port)
    end
    
    dg = DatagramChannel.open
    dg.configureBlocking(false)
    dg.socket.bind(addr)

    b = create_binding
    ec = EventableDatagramChannel.new(dg, b, @my_selector)
    dg.register(@my_selector, SelectionKey::OP_READ, ec)
    @connections.put(b, ec)
    return b
  end

  def send_data(sig, data)
    @connections.get(sig).schedule_outbound_data(ByteBuffer.wrap(data))
  end

  def set_comm_inactivity_timeout(sig, mills)
    @connections.get(sig).setCommInactivityTimeout(mills)
  end

  def send_datagram(sig, data, addr, port=nil)
    @connections.get(sig).schedule_outbound_datagram(ByteBuffer.wrap(data), addr, port)
  end

  def connect_tcp_server(addr, port, bind_addr=nil, bind_port=nil)
    b = create_binding

    begin
      sc = SocketChannel.open
      sc.configureBlocking(false)

      sc.bind(InetSocketAddress.new(bind_addr, bind_port)) if !bind_addr.nil?

      ec = EventableSocketChannel.new(sc, b, @my_selector)

      if sc.connect(InetSocketAddress.new(addr, port))
        raise "immediate-connect unimplemented"
      else
        ec.connect_pending = true
        @connections.put(b, ec)
        @new_connections.add(b)
      end
    rescue IOException => e
      raise "immediate-connect unimplemented"
    end

    return b
  end

  def close_connection(sig, after_writing)
    ec = @connections.get(sig)
    if !ec.nil?
      ec.schedule_close(after_writing)
      @unbound_connections.add(sig)
    end
  end

  def create_binding
    return @binding_index += 1
  end

  def signal_loop_break
    @loop_breaker.set(true)
    @my_selector.wakeup if !@my_selector.nil?
  end

  def start_tls(sig)
    @connections.get(sig).start_tls
  end

  def timer_quantum=(ms)
    if (ms < 5 || ms > 2500)
        raise "attempt to set invalid timer-quantum value: " + ms
    end
    @timer_quantum = ms
  end

  def get_peer_name(sig)
    @connections.get(sig).get_peer_name
  end

  def get_sock_name(sig)
    @connections.get(sig).get_sock_name
  end

  def attach_channel(sc, watch_mode)
    b = create_binding

    ec = EventableSocketChannel.new(sc, b, @my_selector)
    ec.attached = true
    ec.watch_only = true

    @connections.put(b, ec)
    @new_connections.add(b)

    return b
  end

  def detach_channel(sig)
    ec = @connections.get(sig)

    if !ec.nil?
      @unbound_connections.add(sig)
      return ec.channel
    end
  end

  def set_notify_readable(sig, mode)
    @connections.get(sig).notify_readable = mode
  end

  def set_notify_writable(sig, mode)
    @connections.get(sig).notify_writable = mode
  end

  def is_notify_readable(sig)
    @connections.get(sig).notify_readable
  end

  def is_notify_writable(sig)
    @connections.get(sig).is_notify_writable
  end

  def connection_count
    @connections.count + @acceptors.size
  end
  
end
