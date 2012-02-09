-- COMMAND: get-rank
--
-- KEYS[1]: Bid:<campaign_id>:<ad_id>:<keyword_id>

local bid_id = KEYS[1]
local members = KEYS[2] .. ":members"
local ids     = KEYS[2] .. ":ids"
local scores  = KEYS[2] .. ":scores"

function get_rank(id)
  local member = redis.call("HGET", members, id)

  if member then
    return tonumber(redis.call("ZREVRANK", scores, member)) + 1
  end
end

return get_rank(bid_id)
