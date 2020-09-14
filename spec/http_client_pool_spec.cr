require "./spec_helper"
require "./support/http"

describe DB::Pool do
  it "distributes evenly the requests" do
    mutex = Mutex.new
    requests_per_connection = Hash(Socket::Address, Int32).new

    server = HTTP::Server.new do |context|
      remote_address = context.request.remote_address.not_nil!
      mutex.synchronize do
        requests_per_connection[remote_address] ||= 0
        requests_per_connection[remote_address] += 1
      end
      sleep context.request.query_params["delay"].to_f
      context.response.print "ok"
    end
    address = server.bind_unused_port "127.0.0.1"

    run_server(server) do
      fixed_pool_size = 5
      expected_per_connection = 5
      requests = fixed_pool_size * expected_per_connection

      pool = DB::Pool.new(
        initial_pool_size: fixed_pool_size,
        max_pool_size: fixed_pool_size,
        max_idle_pool_size: fixed_pool_size) {
        HTTP::Client.new(URI.parse("http://127.0.0.1:#{address.port}/"))
      }

      done = Channel(Nil).new

      requests.times do
        spawn do
          pool.checkout do |http|
            http.get("/?delay=0.1")
          end
          done.send(nil)
        end
      end

      spawn do
        requests.times { done.receive }
        done.close
      end
      wait_for { done.closed? }

      requests_per_connection.values.should eq([expected_per_connection] * fixed_pool_size)
    end
  end
end
