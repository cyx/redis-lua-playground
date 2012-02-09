require "digest/sha1"
require "redis"

module Lua
  @cache = Hash.new { |h, cmd| h[cmd] = File.read("lua/#{cmd}.lua") }

  def self.call(command, *args)
    begin
      redis.evalsha(sha(command), *args)
    rescue RuntimeError
      redis.eval(@cache[command], *args)
    end
  end

  def self.sha(command)
    Digest::SHA1.hexdigest(@cache[command])
  end

  def self.redis
    @redis ||= Redis.connect
  end
end

if __FILE__ == $0
  Lua.redis.flushdb

  Lua.call("create-bid", 2, "Bid:1:2:3", "Rank:3", "15")
  Lua.call("create-bid", 2, "Bid:1:3:3", "Rank:3", "16")
  Lua.call("create-bid", 2, "Bid:1:4:3", "Rank:3", "15")
  Lua.call("create-bid", 2, "Bid:1:5:3", "Rank:3", "14")


  puts Lua.call("get-rank", 2, "Bid:1:2:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:3:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:4:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:5:3", "Rank:3").inspect

  Lua.call("create-bid", 2, "Bid:1:2:3", "Rank:3", "15")

  puts Lua.call("get-rank", 2, "Bid:1:2:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:3:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:4:3", "Rank:3").inspect
  puts Lua.call("get-rank", 2, "Bid:1:5:3", "Rank:3").inspect
end
