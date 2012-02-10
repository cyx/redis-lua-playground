require "digest/sha1"
require "redis"
require "benchmark"

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

  def self.create_bid(bid_id, rank_prefix, amount)
    members, ids, scores = rank_keys(rank_prefix)

    call("create-bid", 4, bid_id, members, ids, scores, amount, gentimestamp)
  end

  def self.remove_bid(bid_id, rank_prefix)
    members, ids, scores = rank_keys(rank_prefix)

    call("remove-bid", 4, bid_id, members, ids, scores)
  end

  def self.get_rank(bid_id, rank_prefix)
    members, ids, scores = rank_keys(rank_prefix)

    call("get-rank", 4, bid_id, members, ids, scores)
  end

  def self.get_score(bid_id, rank_prefix)
    members, ids, scores = rank_keys(rank_prefix)

    call("get-score", 4, bid_id, members, ids, scores)
  end

  def self.get_index(rank_prefix, n)
    members, ids, scores = rank_keys(rank_prefix)

    call("get-index", 3, members, ids, scores, n)
  end

private
  def self.gentimestamp
    (2 ** 62 - Time.now.to_f * 1_000_000_000).to_i
  end

  def self.rank_keys(rank_prefix)
    ["#{rank_prefix}:members", "#{rank_prefix}:ids", "#{rank_prefix}:scores"]
  end
end

if __FILE__ == $0
  require "cutest"

  prepare do
    Lua.redis.flushdb
  end


  setup do
    Lua.create_bid("Bid:1:2:3", "Rank:3", 15)
    Lua.create_bid("Bid:1:3:3", "Rank:3", 16)
    Lua.create_bid("Bid:1:4:3", "Rank:3", 15)
    Lua.create_bid("Bid:1:5:3", "Rank:3", 14)
  end

  test do
    assert_equal 1, Lua.get_rank("Bid:1:3:3", "Rank:3")
    assert_equal 2, Lua.get_rank("Bid:1:2:3", "Rank:3")
    assert_equal 3, Lua.get_rank("Bid:1:4:3", "Rank:3")
    assert_equal 4, Lua.get_rank("Bid:1:5:3", "Rank:3")
  end

  test do
    Lua.create_bid("Bid:1:2:3", "Rank:3", 15)

    assert_equal 1, Lua.get_rank("Bid:1:3:3", "Rank:3")
    assert_equal 2, Lua.get_rank("Bid:1:2:3", "Rank:3")
    assert_equal 3, Lua.get_rank("Bid:1:4:3", "Rank:3")
    assert_equal 4, Lua.get_rank("Bid:1:5:3", "Rank:3")
  end

  test do
    Lua.create_bid("Bid:1:2:3", "Rank:3", 14)

    assert_equal 1, Lua.get_rank("Bid:1:3:3", "Rank:3")
    assert_equal 4, Lua.get_rank("Bid:1:2:3", "Rank:3")
    assert_equal 2, Lua.get_rank("Bid:1:4:3", "Rank:3")
    assert_equal 3, Lua.get_rank("Bid:1:5:3", "Rank:3")
  end

  test do
    assert_equal "16", Lua.get_score("Bid:1:3:3", "Rank:3")
    assert_equal "15", Lua.get_score("Bid:1:2:3", "Rank:3")
    assert_equal "15", Lua.get_score("Bid:1:4:3", "Rank:3")
    assert_equal "14", Lua.get_score("Bid:1:5:3", "Rank:3")
  end

  test do
    assert_equal "Bid:1:3:3", Lua.get_index("Rank:3", 1)
    assert_equal "Bid:1:2:3", Lua.get_index("Rank:3", 2)
    assert_equal "Bid:1:4:3", Lua.get_index("Rank:3", 3)
    assert_equal "Bid:1:5:3", Lua.get_index("Rank:3", 4)
  end

  test do
    assert Lua.remove_bid("Bid:1:3:3", "Rank:3")
    assert_equal "Bid:1:2:3", Lua.get_index("Rank:3", 1)

    assert Lua.remove_bid("Bid:1:2:3", "Rank:3")
    assert_equal "Bid:1:4:3", Lua.get_index("Rank:3", 1)

    assert Lua.remove_bid("Bid:1:4:3", "Rank:3")
    assert_equal "Bid:1:5:3", Lua.get_index("Rank:3", 1)

    assert Lua.remove_bid("Bid:1:5:3", "Rank:3")
    assert_equal nil, Lua.get_index("Rank:3", 1)

    # Trying to remove non existent bid returns a NIL reply.
    assert_equal nil, Lua.remove_bid("Bid:1:5:3", "Rank:3")
  end

  test do
    threads = 1000.times.map do |i|
      Thread.new(i) do |amount|
        Lua.create_bid("Bid:1:2:3", "Rank:3", amount)
      end
    end

    t = Benchmark.realtime { threads.each(&:join) }

    assert t < 0.30
  end
end
