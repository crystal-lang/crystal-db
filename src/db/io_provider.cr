module DB
  # An `IOProvider` can be used to customize
  # how underlying IO for connections are created.
  # Not all drivers are backed by IO.
  #
  # The setup and teardown methods will be called once
  # per Database Connection Pool when DB.open is used,
  # and once for the single connection when DB.connect is used.
  abstract class IOProvider
    abstract def setup : Void

    abstract def teardown : Void

    abstract def build_io : IO
  end
end
