require "http"
require "../src/db/pool"

pool = DB::Pool.new { HTTP::Client.new(URI.parse("https://google.com")) }

pool.checkout do |http|
  pp http.get("/")
end
