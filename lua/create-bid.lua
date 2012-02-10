-- COMMAND: create-bid
--
-- KEYS[1]: Bid:<campaign_id>:<ad_id>:<keyword_id>
-- KEYS[2]: Ranks:<keyword_id>:members
-- KEYS[3]: Ranks:<keyword_id>:ids
-- KEYS[4]: Ranks:<keyword_id>:scores
--
-- ARGV[1]: amount, e.g. 15 (in 1000 precision)
-- ARGV[2]: inverted timestamp

local bid_id    = KEYS[1]
local members   = KEYS[2]
local ids       = KEYS[3]
local scores    = KEYS[4]
local amount    = ARGV[1]
local timestamp = ARGV[2]
local newmember = timestamp .. ":" .. bid_id

function del_rank(id, member)
  redis.call("ZREM", scores, member)
  redis.call("HDEL", members, id)
  redis.call("HDEL", ids, member)
end

function add_rank(id, score)
  redis.call("HSET", members, id, newmember)
  redis.call("HSET", ids, newmember, id)
  redis.call("ZADD", scores, score, newmember)
end

function get_rank(id)
  local member = redis.call("HGET", members, id)

  if member then
    return tonumber(redis.call("ZREVRANK", scores, member)) + 1
  end
end

local member = redis.call("HGET", members, bid_id)
local score  = redis.call("ZSCORE", scores, member)

if tonumber(score) ~= tonumber(amount) then
  if member then
    del_rank(bid_id, member)
  end

  add_rank(bid_id, amount)
end

return get_rank(bid_id)
