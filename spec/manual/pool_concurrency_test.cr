# This file is to be executed as:
#
# % crystal run --release [-Dpreview_mt] ./spec/manual/pool_concurrency_test.cr -- --options="max_pool_size=5" --duration=30 --concurrency=4
#
#

require "option_parser"
require "../dummy_driver"
require "../../src/db"

options = ""
duration = 3
concurrency = 4

OptionParser.parse do |parser|
  parser.banner = "Usage: pool_concurrency_test [arguments]"
  parser.on("-o", "--options=VALUE", "Connection string options") { |v| options = v }
  parser.on("-d", "--duration=SECONDS", "Specifies the duration in seconds") { |v| duration = v.to_i }
  parser.on("-c", "--concurrency=VALUE", "Specifies the concurrent requests to perform") { |v| concurrency = v.to_i }
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit
  end
  parser.invalid_option do |flag|
    STDERR.puts "ERROR: #{flag} is not a valid option."
    STDERR.puts parser
    exit(1)
  end
end

multi_threaded = {% if flag?(:preview_mt) %} ENV["CRYSTAL_WORKERS"]?.try(&.to_i?) || 4 {% else %} false {% end %}
release = {% if flag?(:release) %} true {% else %} false {% end %}

if !release
  puts "WARNING: This should be run in release mode."
end

db = DB.open "dummy://host?#{options}"

start_time = Time.monotonic

puts "Starting test for #{duration} seconds..."

concurrency.times do
  spawn do
    loop do
      db.scalar "1"
      Fiber.yield
    end
  end
end

sleep duration.seconds

end_time = Time.monotonic

puts "          Options : #{options}"
puts "   Duration (sec) : #{duration} (actual #{end_time - start_time})"
puts "      Concurrency : #{concurrency}"
puts "   Multi Threaded : #{multi_threaded ? "Yes (#{multi_threaded})" : "No"}"
puts "Total Connections : #{DummyDriver::DummyConnection.connections_count}"
puts " Total Statements : #{DummyDriver::DummyStatement.statements_count}"
puts "    Total Queries : #{DummyDriver::DummyStatement.statements_exec_count}"
puts " Throughput (q/s) : #{DummyDriver::DummyStatement.statements_exec_count / duration}"

if !release
  puts "WARNING: This should be run in release mode."
end
