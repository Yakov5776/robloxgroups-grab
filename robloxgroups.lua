local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

io.stdout:setvbuf("no") -- So prints are not buffered - http://lua.2524044.n2.nabble.com/print-stdout-and-flush-td6406981.html

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    print("discovered", item)
    target[item] = true

    return true
  end

  return false
end

find_item = function(url)
  local gid = string.match(url, "^https?://groups%.roblox%.com/v1/groups/([0-9]+)")
  if gid then
    return {
      type = "group-meta",
      value = gid
    }
  end

  local members_id, members_cursor = string.match(url, "^https?://groups%.roblox%.com/v1/groups/([0-9]+)/users%?limit=100&cursor=(.*)$")
  if members_id then
    return {
      type = "group-members-cursored",
      value = members_id .. ":" .. members_cursor
    }
  end

  local wall_id, wall_cursor = string.match(url, "^https?://groups%.roblox%.com/v2/groups/([0-9]+)/wall/posts%?limit=100&cursor=(.*)$")
  if wall_id then
    return {
      type = "group-wall-cursored",
      value = wall_id .. ":" .. wall_cursor
    }
  end


  -- testing
  -- io.stdout:write("item is "..type_..":"..value.."\n")
  -- io.stdout:flush()
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {["templates"]={}, ["ignore"]={}}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if context["ignore"][url]
    or context["ignore"][string.match(url, "^([^%?]+)")] then
    return false
  end

  if string.match(url, "^https?://[^/]*rbxcdn.com/") then
    return item_type == "group-meta"
  end

  if not string.match(url, "^https?://[^/]*roblox%.com")
          and not string.match(url, "^https?://[^/]*rbxcdn%.com/") then
    error("Unknown domain on URL " .. url)
    return false
  else
    return true
  end

  for _, pattern in pairs({
    "([0-9a-zA-Z%-_]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local file_contents = read_file(file)


  if item_type == "group-meta" then
    if string.match(url, "^https?://groups%.roblox%.com/v1/groups/([0-9]+)/?$") then
      if status_code ~= 400 then
        check("https://thumbnails.roblox.com/v1/groups/icons?groupIds=" ..
            item_value .. "&size=420x420&format=Webp&isCircular=false")
        check("https://thumbnails.roblox.com/v1/groups/icons?groupIds=" ..
            item_value .. "&size=420x420&format=Png&isCircular=false")

        -- What used to be separate initial items
        check("https://groups.roblox.com/v1/groups/" .. item_value .. "/roles")
        check("https://apis.roblox.com/community-links/v1/groups/" .. item_value .. "/shout")
        check("https://groups.roblox.com/v1/featured-content/event?groupId=" .. item_value)
        check("https://groups.roblox.com/v1/groups/" .. item_value .. "/name-history?limit=100&sortOrder=Asc")
        check("https://groups.roblox.com/v1/groups/" .. item_value .. "/users?limit=100&sortOrder=Asc")
        check("https://groups.roblox.com/v2/groups/" .. item_value .. "/wall/posts?limit=100&sortOrder=Asc")
        
        local name_cleaned = cjson.decode(file_contents)["name"]:gsub("'", ""):gsub("[^a-zA-Z0-9]+", "-"):gsub("^%-", ""):gsub("%-$", "")
        if name_cleaned == "" then
          name_cleaned = "unnamed"
        end
        check("https://www.roblox.com/communities/" .. item_value .. "/" .. name_cleaned)
      else
        assert(cjson.decode(file_contents)["errors"][1]["message"] == "Group is invalid or does not exist.")
      end
    elseif string.match(url, "^https?://thumbnails%.roblox%.com/v1/groups/icons") then
      local json_data = cjson.decode(file_contents)["data"]
      if #json_data > 0 then
        check(json_data[1]["imageUrl"])
      end
    end
  end



  -- These apply for the initial page and the cursored versions
  local wall_id = string.match(url, "^https?://groups%.roblox%.com/v2/groups/(%d+)/wall/posts%?")
  if wall_id and status_code ~= 403 and status_code ~= 429 then
    local nextpagecursor = cjson.decode(file_contents)["nextPageCursor"]

    if nextpagecursor ~= cjson.null then
      discover_item(discovered_items, "group-wall-cursored:"..wall_id..":"..nextpagecursor)
    end
  end

  local namehistory_id = string.match(url, "^https?://groups%.roblox%.com/v1/groups/(%d+)/name-history%?")
  if namehistory_id and status_code ~= 429 then
    local nextpagecursor = cjson.decode(file_contents)["nextPageCursor"]

    if nextpagecursor ~= cjson.null then
      error("This has not been encountered in testing, failing for now")
    end
  end

  local members_id = string.match(url, "^https?://groups.roblox.com/v1/groups/(%d+)/users%?")
  if members_id and status_code ~= 429 then
    local nextpagecursor = cjson.decode(file_contents)["nextPageCursor"]

    if nextpagecursor ~= cjson.null then
      discover_item(discovered_items, "group-members-cursored:"..members_id..":"..nextpagecursor)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false

  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404
    and http_stat["statcode"] ~= 403
    and http_stat["statcode"] ~= 400 then
    retry_url = true
    return false
  end

  if http_stat["len"] == 0 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response (" .. err .. "). ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 4
    if status_code == 429 then
      maxtries = 6
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(3, tries-0.5)),
      math.floor(math.pow(3, tries))
    )
    if status_code == 429 then
      sleep_time = sleep_time + 700
    end
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    if os.getenv("DO_DEBUG") and os.getenv("DO_DEBUG") ~= "" then
      print("Skipping submitting items for " .. key)
      return
    end
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["robloxgroups-tbs748r6rw99euh6"] = discovered_items,
    --["urls-"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  -- Stop 429s in testing
  if os.getenv("DO_DEBUG") and os.getenv("DO_DEBUG") ~= "" then
    print("Sleeping to avoid the rate limit")
    os.execute("sleep 60")
    return
  end
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

