module DB
  class Error < Exception
  end

  class MappingException < Exception
  end

  class PoolTimeout < Error
  end
end
