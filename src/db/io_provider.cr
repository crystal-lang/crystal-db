module DB
  # An `IOProvider` can be used to customize
  # how underlying IO for connections are created.
  # Not all drivers are backed by IO.
  abstract class IOProvider
    abstract def setup : Void

    abstract def teardown : Void

    abstract def build_io : IO
  end
end
