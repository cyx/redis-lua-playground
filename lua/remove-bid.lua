-- COMMAND: remove-bid
--
-- KEYS[1]: Bid:<campaign_id>:<ad_id>:<keyword_id>
-- KEYS[2]: Ranks:<keyword_id>:members
-- KEYS[3]: Ranks:<keyword_id>:ids
-- KEYS[4]: Ranks:<keyword_id>:scores

local bid_id    = KEYS[1]
local members   = KEYS[2]
local ids       = KEYS[3]
local scores    = KEYS[4]

function del_rank(id, member)
  redis.call("ZREM", scores, member)
  redis.call("HDEL", members, id)
  redis.call("HDEL", ids, member)
end

local member = redis.call("HGET", members, bid_id)

if member then
  del_rank(bid_id, member)
  return true
else
  return false
end
