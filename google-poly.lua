dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local bad_items = {}

local discovered = {}
local allowed_urls = {}

local reqid = nil
local version = nil

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

get_item = function(url)
  local match = string.match(url, "^https?://poly%.google%.com/view/([0-9a-zA-Z%-_]+)$")
  local type_ = "poly"
  if not match then
    match = string.match(url, "^https?://poly%.google%.com/user/([0-9a-zA-Z%-_]+)$")
    type_ = "user"
  end
  if match and type_ then
    return match, type_
  end
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
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

queue_item = function(value_, type_)
  if value_ == item_value or value_ == nil or type_ == nil then
    return nil
  end
  local new_item = type_ .. ":" .. value_
  if discovered[new_item] then
    return nil
  end
  io.stdout:write("Found new item " .. new_item .. ".\n")
  io.stdout:flush()
  discovered[new_item] = true
end

allowed = function(url, parenturl)
  if parenturl and string.match(parenturl, "%.gltf$") then
    allowed_urls[url] = true
  end

  if allowed_urls[url] then
    return true
  end

  if string.match(urlparse.unescape(url), "[<>\\%*%$%^%[%],%(%){}]")
    or string.match(url, "^https?://accounts%.google%.com/") then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if not tested[s] then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  queue_item(get_item(url))

  for s in string.gmatch(url, "([0-9a-zA-Z%-_]+)") do
    if ids[s] then
      return true
    end
  end

  if string.match(url, "batchexecute") then
    return true
  end

  if string.match(url, "^https?://[^/]*googleusercontent%.com/") then
    print(url)
 
 --   return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.gsub(url_, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])", function (s)
      local i = tonumber(s, 16)
      if i < 128 then
        return string.char(i)
      else
        -- should not have these
        abort_item()
      end
    end)
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      if string.match(url, "^https?://poly%.google%.com/") then
        checknewurl(string.match(newurl, "^%.(/.*)$"))
      else
        check(urlparse.absolute(url, newurl))
      end
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local match = string.match(url, "^(https?://[^/]*googleusercontent%.com/.+)=w[0-9]+%-h[0-9]+")
  if match then
    check(match)
  end

  if allowed(url, nil) and status_code == 200
    and (
      not string.match(url, "^https?://[^/]*googleusercontent%.com/")
      or string.match(url, "%.gltf")
    ) then
    html = read_file(file)
    if string.match(url, "%.gltf$") then
      for s in string.gmatch(html, '"([a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9]%-[a-f0-9][a-f0-9][a-f0-9][a-f0-9]%-[a-f0-9][a-f0-9][a-f0-9][a-f0-9]%-[a-f0-9][a-f0-9][a-f0-9][a-f0-9]%-[a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9][a-f0-9])"') do
        ids[s] = true
        check("https://www.tiltbrush.com/environments/" .. s .. "/" .. s .. ".gltf")
        check("https://www.tiltbrush.com/environments/" .. s .. "/" .. s .. ".bin")
      end
    end
    if string.match(url, "batchexecute") then
      local count = 0
      for s in string.gmatch(html, '%[\\"([^\\]+)\\"') do
        if string.len(s) < 15 then
          count = count + 1
          queue_item(s, "poly")
        end
      end
      if count == 0 then
        return urls
      end
    end
    if string.match(url, "/user/") or string.match(url, "batchexecute") then
      local s = string.match(html, "{key:%s*'ds:1'%s*,%s*isError:%s*false%s*,%s*hash:%s*'2'%s*,%s*data:%[\"([^\"]+)")
      if not s then
        s = string.match(html, '"EWfySd"%s*,%s*"%[\\"(.-)\\"')
      end
      if not reqid then
        reqid = string.match(html, '"FdrFJe":"([^"]+)"')
      end
      if not version then
        version = string.match(html, '"cfb2h":"([^"]+)"')
      end
      local data = '[[["EWfySd","[30,\\"' .. s .. '\\",null,[],\\"' .. item_value .. '\\",null,null,[],null,null,null,null,\\"created\\",null,[]]",null,"generic"]]]'
      print(data)
      data = string.gsub(
        data, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
        function (s)
          local i = tonumber(s, 16)
          if i < 128 then
            return string.char(i)
          else
            -- should not have these
            abort_item()
          end
        end
      )
      print(data)
      data = string.gsub(
        data, "([^A-Za-z0-9_])",
        function(s)
          return string.format("%%%02X", string.byte(s))
        end
      )
      print(data)
      table.insert(
        urls,
        {
          url="https://poly.google.com/_/VrZandriaUi/data/batchexecute?rpcids=EWfySd&f.sid=" .. reqid .. "&bl=" .. version .. "&hl=nl&soc-app=653&soc-platform=1&soc-device=1&rt=c",
          method="POST",
          body_data="f.req=" .. data .. "&",
          headers={
            ["content-type"]="application/x-www-form-urlencoded;charset=UTF-8"
          }
        }
      )
    end
    html = string.gsub(html, '\\"', '"')
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  local new_item_value, new_item_type = get_item(url["url"])
  if new_item_value then
    abortgrab = false
    ids[new_item_value] = true
    item_value = new_item_value
    item_type = new_item_type
    item_name = new_item_type .. ":" .. new_item_value
    io.stdout:write("Archiving item " .. item_name .. ".\n")
    io.stdout:flush()
  end
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if downloaded[newloc] or addedtolist[newloc]
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.ABORT
  end

  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 1
    if not allowed(url["url"], nil) then
      maxtries = 3
    end
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  local items = nil
  for item, _ in pairs(discovered) do
    print('found item', item)
    if items == nil then
      items = item
    else
      items = items .. "\0" .. item
    end
  end
  if items ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/google-poly-gevcr9z0irsjhim/",
        items
      )
      if code == 200 or code == 409 then
        break
      end
      io.stdout:write("Could not queue new items.\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abortgrab = true
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
    return wget.exits.IO_FAIL
  end
  return exit_status
end

