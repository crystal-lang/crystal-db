require "../src/db/pool"
require "uuid"

class DummyIO < IO
  def read(slice : Bytes)
    sleep rand.seconds
  end

  def write(slice : Bytes) : Nil
    sleep rand.seconds # simulate I/O yielding the CPU
    STDOUT.puts "wrote #{String.new(slice)}"
  end
end

pool = DB::Pool.new { DummyIO.new }
channel = Channel(Nil).new
count = 10

count.times do
  spawn do
    pool.checkout do |io|
      io << "hello"
    end
    channel.send nil
  end
end

count.times { channel.receive }

pool.close
