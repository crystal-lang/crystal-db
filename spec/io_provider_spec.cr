require "./spec_helper"
require "log/spec"

class DummyIOProvider < DB::IOProvider
  @setup_called = false
  @teardown_called = false

  def setup : Void
    raise "setup called twice" if @setup_called
    @setup_called = true
    DB::Log.debug &.emit("DummyIOProvider#setup")
  end

  def teardown : Void
    raise "teardown called twice" if @teardown_called
    @teardown_called = true
    DB::Log.debug &.emit("DummyIOProvider#teardown")
  end

  def build_io : IO
    DB::Log.debug &.emit("DummyIOProvider#build_io")
    return IO::Memory.new
  end
end

describe DB::IOProvider do
  it "setup/teardown are called for pool connection" do
    Log.capture(DB::Log.source) do |logs|
      DB.open "dummy://host", io_provider: DummyIOProvider.new do |db|
        cnn1 = db.checkout
        cnn2 = db.checkout

        db.release(cnn1)
        db.release(cnn2)
      end

      logs.check(:debug, /DummyIOProvider#setup/i)
      logs.check(:debug, /DummyIOProvider#build_io/i)
      logs.check(:debug, /DummyIOProvider#build_io/i)
      logs.check(:debug, /DummyIOProvider#teardown/i)
    end
  end

  it "setup/teardown are called for single connection" do
    Log.capture(DB::Log.source) do |logs|
      DB.connect "dummy://host", io_provider: DummyIOProvider.new do |cnn|
      end

      logs.check(:debug, /DummyIOProvider#setup/i)
      logs.check(:debug, /DummyIOProvider#build_io/i)
      logs.check(:debug, /DummyIOProvider#teardown/i)
    end
  end
end
