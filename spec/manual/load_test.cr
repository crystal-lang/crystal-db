# This file is to be executed as:
#
# % crystal ./spec/manual/load_test.cr
#
# It generates a number of producers and consumers. If the process hangs
# it means that the connection pool is not working properly. Likely a race condition.
#

require "../dummy_driver"
require "../../src/db"
require "json"

CONNECTION = "dummy://host?initial_pool_size=5&max_pool_size=5&max_idle_pool_size=5"

alias TChannel = Channel(Int32)
alias TDone = Channel(Bool)

COUNT = 200

def start_consumer(channel : TChannel, done : TDone)
  spawn do
    indeces = Set(Int32).new
    loop do
      indeces << channel.receive
      puts "Received size=#{indeces.size}"
      break if indeces.size == COUNT
    end
    done.send true
  end
end

def start_producers(channel : TChannel)
  db = DB.open CONNECTION do |db|
    sql = "1,title,description,2023 " * 100_000

    COUNT.times do |index|
      spawn(name: "prod #{index}") do
        puts "Sending #{index}"
        _films = db.query_all(sql, as: {Int32, String, String, Int32})
      rescue ex
        puts "Error: #{ex.message}"
      ensure
        channel.send index
      end
    end
  end
end

channel = TChannel.new
done = TDone.new
start_consumer(channel, done)
start_producers(channel)

done.receive
