-- COMMAND: get-index
--
-- KEYS[1]: Rank:<keyword id>:members
-- KEYS[2]: Rank:<keyword id>:ids
-- KEYS[3]: Rank:<keyword id>:scores
--
-- ARGV[1]: N the index which we want to get the id of

local members = KEYS[1]
local ids     = KEYS[2]
local scores  = KEYS[3]
local N       = tonumber(ARGV[1]) - 1

function get_index(n)
  local member = redis.call("ZREVRANGE", scores, n, n)[1]

  if member then
    return redis.call("HGET", ids, member)
  end
end

return get_index(N)
