class Packet

  attr_reader :bb, :recipient
  
  def initialize(bb, recipient)
    @bb, @recipient = bb, recipient
  end

end
