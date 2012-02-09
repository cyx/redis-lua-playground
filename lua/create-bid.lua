-- COMMAND: create-bid
--
-- KEYS[1]: Bid:<campaign_id>:<ad_id>:<keyword_id>
-- KEYS[2]: Ranks:<keyword_id>
-- ARGV[1]: amount, e.g. 15 (in 1000 precision)

local UPPER   = 2 ^ 32
local bid_id  = KEYS[1]
local members = KEYS[2] .. ":members"
local ids     = KEYS[2] .. ":ids"
local scores  = KEYS[2] .. ":scores"
local amount  = ARGV[1]

function generate_member(id)
  local stamp = redis.call("incr", "Ranks:stamp")

  return tostring(UPPER - stamp) .. ":" .. id
end

function del_rank(id, member)
  redis.call("ZREM", scores, member)
  redis.call("HDEL", members, id)
  redis.call("HDEL", ids, member)
end

function add_rank(id, score)
  local member = generate_member(id)

  redis.call("HSET", members, id, member)
  redis.call("HSET", ids, member, id)
  redis.call("ZADD", scores, score, member)
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
