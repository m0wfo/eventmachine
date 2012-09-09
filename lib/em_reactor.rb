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
    @connection = HashMap.new
    @acceptors = HashMap.new
    @new_connections = ArrayList.new
    @unbound_connections = ArrayList.new
    @detached_connections = ArrayList.new

    @binding_index = 0
    @loop_breaker = AtomicBoolean.new
    @loop_breaker.set(false)

    @my_read_buffer = ByteBuffer.allocate(32*1024)
    @timer_quantum = 98
  end

  def event_callback(sig, event_type, data, data2=nil)
    puts "Default callback: #{sig}#{event_type}#{data}#{data2}"
  end

  def run
    begin
      @my_selector = Selector.open()
      @b_run_reactor = true
    rescue (IOException e)
      p 'err!'
    end

    while @b_run_reactor
      run_loop_breaks
      break if !@b_run_reactor

      run_timers
      break if !@b_run_reactor

      remove_unbound_connections
      check_io
      add_new_connections
      process_io
    end

    close
  end

  def add_new_connections
    iter = @unbound_connections.listIterator(0)
    while iter.hasNext()
      ec = iter.next()
      ec.cleanup()
    end
    @detached_connections.clear()

    iter2 = @new_connections.listIterator(0)
    while iter2.hasNext()
      b = iter2.next()

      ec = @connections.get(b)
      if !ec
        begin
          ec.register()
        rescue ClosedChannelException e
          @unbound_connections.add(ec.getBinding())
        end
      end
    end
    @new_connections.clear()
  end

  def remove_unbound_connections
    iter = @unbound_connections.listIterator(0)
    while iter.hasNext()
      b = iter.next()
      ec = @connections.remove(b)

      if !ec
        eventCallback(b, EM_CONNECTION_UNBOUND, nil)
        ec.close()

        if ec.nil? && ec.isAttached()
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
      now = java.lang.Date.new.getTime()
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
      rescue IOException e
        e.printStackTrace
      end
    end
  end

  def process_io
    it = @my_selector.selectedKeys().iterator()
    while it.hasNext()
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
    ss = key.channel()

    10.times do
      begin
        sn = ss.accept()
        break if sn.nil?
      rescue IOException e
        e.printStackTrace()
        key.cancel()

        server = @acceptors.remove(key.attachment())
        server.close() if !server.nil?
        break
      end

      begin
        sn.configureBlocking(false)
      rescue IOException e
        e.printStackTrace()
        next
      end

      b = create_binding
      ec = EventableSocketChannel.new(sn, b, @my_selector)
      @connections.put(b, ec)
      @new_connections.add(b)

      event_callback(key.attachment().longValue(), EM_CONNECTION_ACCEPTED, nil, b)
    end
  end

  def is_readable(key)
    ec = key.attachment()
    b = ec.getBinding()

    if ec.isWatchOnly()
      event_callback(b, EM_CONNECTION_READ, @my_read_buffer) if ec.isNotifyReadable()
    else
      @my_read_buffer.clear()

      begin
        ec.readInboundData(@my_read_buffer)
        @my_read_buffer.flip()
        event_callback(b, EM_CONNECTION_READ, @my_read_buffer)
      rescue IOException e
        @unbound_connections.add(b)
      end
    end
  end

  def is_writable(key)
    ec = key.attachment()
    b = ec.getBinding()

    if ec.isWatchOnly()
      event_callback(b, EM_CONNECTION_NOTIFY_WRITABLE, nil) if ec.isNotifyWritable()
    else
      begin
        @unbound_connections.add(b) if !ec.writeOutboundData()
      rescue IOException e
        @unbound_connections.add(b)
      end
    end
  end

  def is_connectable(key)
    ec = key.attachment()
    b = ec.getBinding()
  end

  def close
  end

  def run_loop_breaks
  end

  def stop
  end

  def run_timers
  end

  def install_oneshot_timer
  end

  def start_tcp_server
  end

  def stop_tcp_server
  end

  def open_udp_socket
  end

  def send_data
  end

  def set_comm_inactivity_timeout(sig, mills)
    @connections.get(sig).setCommInactivityTimeout(mills)
  end

  def send_datagram
  end

  def connect_tcp_server
  end

  def close_connection
  end

  def create_binding
  end

  def signal_loop_break
  end

  def start_tls
  end

  def set_timer_quantum
  end

  def get_peer_name(sig)
  end

  def get_sock_name(sig)
  end

  def attach_channel(sc, watch_mode)
  end

  def detach_channel(sig)
  end

  def set_notify_readable(sig, mode)
  end

  def set_notify_writable(sig, mode)
  end

  def is_notify_readable(sig)
  end

  def is_notify_writable(sig)
  end

  def connection_count
  end
  
end
