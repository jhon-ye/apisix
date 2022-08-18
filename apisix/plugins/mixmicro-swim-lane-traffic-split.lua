--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local core = require("apisix.core")

local plugin_name = "mixmicro-swim-lane-traffic-split"
local schema = {
    type = "object",
    properties = {
        service_name = {type = "string"}
    },
    required = {"service_name"}
}

local req_read_body = ngx.req.read_body
local req_get_post_args = ngx.req.get_post_args
local req_get_body_data = ngx.req.get_body_data
local ngx = ngx
local authorization = "6d07f81a0c9d0757b0714174a397f5ab"
local tags = {"x_group", "x_gray_tag", "x_swim_lane_tag", "x_pressure_tag"}
local upstream = require("apisix.upstream")
local ipmatcher  = require("resty.ipmatcher")
local init       = require("apisix.init")
local pairs = pairs
local base64_encode = require("base64").encode
local traffic_api_schema = {
    type = "object",
    properties = {
        service_name = {type = "string"},
        swim_service_host = {type = "string"},
        swim_service_port = {type = "integer"},
        id = {type = "integer"},
        x_group = {type = "string"},
        x_gray_tag = {type = "string"},
        x_swim_lane_tag = {type = "string"},
        x_pressure_tag = {type = "string"}
    },
    required = {"service_name", "swim_service_host", "id", "x_group", "x_swim_lane_tag" ,"swim_service_port"}
}
local exchange = require("apisix.plugins.mixmicro-utils.mixmicro-utils-exchange")
local string = string

local _M = {
    version = 0.1,
    priority = 967,
    name = plugin_name,
    schema = schema,
}


function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end

    return true
end

local function get_args()
    local ctx = ngx.ctx.api_ctx
    local args, err
    req_read_body()
    if string.find(ctx.var.http_content_type or "","application/json",
                   1, true) then
        local req_body = req_get_body_data()
        args, err = core.json.decode(req_body)
        if err then
            core.log.error("json.decode(", req_body, ") failed! ", err)
        end
    else
        args = req_get_post_args()
    end

    return args
end

local function parse_domain_for_node(node)
    if not ipmatcher.parse_ipv4(node)
       and not ipmatcher.parse_ipv6(node)
    then
        local ip, err = init.parse_domain(node)
        if ip then
            return ip
        end

        if err then
            return nil, err
        end
    end

    return node
end

local function fix_swim_lane(swim_body, replace)
    local lane = ""
    for _, v in pairs(tags) do
        local tag_value = ""
        local tag = v
        if replace then
           tag = string.gsub(tag, "_", "-")
        end

        if swim_body[tag] then
            tag_value = swim_body[tag]
        end

        lane = lane .. v .. ":" .. string.lower(tag_value) .. ";"
    end
    return lane
end

local function valid_traffic_api()
    local headers = ngx.req.get_headers()
    local app_key = headers["X-API-KEY"]
    if not app_key or app_key ~= authorization then
        return 401, {message = "no authorization"}
    end

    return 200
end

local function remove_item(id, values)
    for k, v in pairs(values) do
        if v.id == id then
            values[k] = nil
        end
    end
end

local function add_traffic_service()
    local ok, err = valid_traffic_api()
    if ok ~= 200 then
        core.response.exit(403, err)
    end

    local args = get_args()
    ok, err = core.schema.check(traffic_api_schema, args)
    if not ok then
        core.response.exit(403, {message = err})
    end

    local service_name = string.lower(args.service_name)
    local key = "/mixmicro/service/" .. service_name
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("failed to get mixmicro traffic[", key, "]: ", err)
        core.response.exit(500,  {message = err})
    end

    local kv = res.body.node
    local data = {}
    if kv and kv.value then
        data = kv.value
    end

    core.log.warn("item:" .. core.json.encode(args))
    local lane = fix_swim_lane(args)
    args.lane = lane
    remove_item(args.id, data)
    data[lane] = args
    core.etcd.set(key ,data)
    core.response.exit(200, {message = "success"})
end

local function  get_traffic_service()
    local ok, err = valid_traffic_api()
    if ok ~= 200 then
        core.response.exit(403, err)
    end

    local service_name = exchange.get_url_parameter_value("service_name")
    if not service_name then
        core.response.exit(403, {message = "service not found"})
    end

    local key = "/mixmicro/service/" .. service_name
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("failed to get mixmicro traffic[", key, "]: ", err)
        core.response.exit(500,  {message = err})
    else
        if not res.body or not res.body.node then
            core.response.exit(404, {message = "service not found"})
            return
        end
        core.response.exit(200, res.body.node)
    end
end

local function delete_traffic_serivce()
    local ok, err = valid_traffic_api()
    if ok ~= 200 then
        core.response.exit(403, err)
    end

    local service_name = exchange.get_url_parameter_value("service_name")
    if not service_name then
        core.response.exit(403, {message = "service not found"})
    end

    local key = "/mixmicro/service/" .. service_name
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("failed to get mixmicro traffic[", key, "]: ", err)
        core.response.exit(500,  {message = err})
    end

    if not res.body then
        core.response.exit(404, {message = "service not found"})
    end

    local gray_service_id = exchange.get_url_parameter_value("gray_service_id")
    if  gray_service_id and res.body.node then
       local values =  remove_item(gray_service_id, res.body.node)
        core.response.exit(200, values)
        return
    else
        core.etcd.delete(key)
        core.response.exit(200, {message = "success"})
        return
    end
end

local function set_upstream(swim_node, ctx)
    local new_nodes = {}
    local node = {}
    node.host = parse_domain_for_node(swim_node.swim_service_host)
    node.port = swim_node.swim_service_port
    node.weight = 100
    core.table.insert(new_nodes, node)
    core.log.warn("upstream_host: ", ctx.var.upstream_host)

    local up_conf = {
        name = swim_node.service_name,
        type = "roundrobin",
        nodes = new_nodes,
        timeout = {
            send = 30,
            read = 30,
            connect = 30
        }
    }

    local ok, err = upstream.check_schema(up_conf)
    if not ok then
        core.log.error("failed to validate generated upstream: ", err)
        return 500, err
    end

    local matched_route = ctx.matched_route
    up_conf.parent = matched_route
    local upstream_key = up_conf.type .. "#route_" ..
                         matched_route.value.id .. "_" ..swim_node.id
    core.log.warn("upstream_key: ", upstream_key, "; conf_info: ", core.json.encode(up_conf), "; swim_node: ", core.json.encode(swim_node))
    upstream.set(ctx, upstream_key, ctx.conf_version, up_conf)
end

local function set_service_gov_header(swim_node, ctx)
    local sw8_header
    if swim_node.x_swim_lane_tag then
        sw8_header =  base64_encode("x-swim-lane-tag") .. ":" .. base64_encode(swim_node.x_swim_lane_tag)
    end

    if swim_node.x_pressure_tag then
        sw8_header = sw8_header .. "," .. base64_encode("x-pressure-tag") .. ":" .. base64_encode(swim_node.x_pressure_tag)
    end

    core.log.warn("service gov header sw8-s: ", sw8_header)
    core.request.set_header(ctx, "sw8-s", sw8_header)
end

function _M.access(conf, ctx)
    local key = "/mixmicro/service/" .. conf.service_name
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("swim service:[", conf.service_name, "]: ", "find from etcd err: ", err)
        return
    else
        if not res.body or not res.body.node  or not res.body.node.value then
            core.log.warn("swim service:[", conf.service_name, "]", "not find from etcd")
            return
        end
    end

    local values = res.body.node.value
    local lane = fix_swim_lane(ngx.req.get_headers(), true)
    local swim_node = values[lane]
    if not swim_node then
        core.log.warn("can not find swim lane: ", lane, "for service:[", conf.service_name, "]")
        return
    end

    set_upstream(swim_node, ctx)
    set_service_gov_header(swim_node, ctx)
end

function _M.api()
    local local_conf = core.config.local_conf()
    local attr = core.table.try_read_attr(local_conf, "plugin_attr",
                                          plugin_name)
    local path = "/apisix/swim-lane/traffic-split"
    if attr and attr.authorization then
        authorization = attr.authorization
    end

    return {
        {
            methods = {"POST"},
            uri = path,
            handler = add_traffic_service
        },
        {
            methods = {"GET"},
            uri = path,
            handler = get_traffic_service
        },
        {
            methods = {"DELETE"},
            uri = path,
            handler = delete_traffic_serivce
        }
    }
end

return _M
