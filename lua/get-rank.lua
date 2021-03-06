-- COMMAND: get-rank
--
-- KEYS[1]: Bid:<campaign_id>:<ad_id>:<keyword_id>
-- KEYS[2]: Rank:<keyword id>:members
-- KEYS[3]: Rank:<keyword id>:ids
-- KEYS[4]: Rank:<keyword id>:scores

local bid_id  = KEYS[1]
local members = KEYS[2]
local ids     = KEYS[3]
local scores  = KEYS[4]

function get_rank(id)
  local member = redis.call("HGET", members, id)

  if member then
    return tonumber(redis.call("ZREVRANK", scores, member)) + 1
  end
end

return get_rank(bid_id)
