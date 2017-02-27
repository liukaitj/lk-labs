local cjson = require ("cjson")
local kafka_producer = require ("resty.kafka.producer")

-- 获取请求信息
local log_json = {}
log_json["http_user_agent"] = ngx.var.http_user_agent
log_json["request_body"] = ngx.var.request_body

local message = cjson.encode(log_json)

-- 定义kafka客户端信息
local broker_list = {
  { host = "172.16.181.155", port = 9092 },
}

--local producer = kafka_producer:new(broker_list, { producer_type = "async" })
local producer = kafka_producer:new(broker_list)

-- 向kafka发送消息
local key = "key"
local offset, err = producer:send("test", key, message)
if offset == nil or not offset then
  -- ngx.log(ngx.ERR, "kafka send err:", err)
  ngx.say("kafka send err:", err)
  return
end
ngx.say("send success, offset: ", tonumber(offset))
