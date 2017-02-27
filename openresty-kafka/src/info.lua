local cjson = require ("cjson")
local client = require ("resty.kafka.client")

local broker_list = {
  { host = "127.0.0.1", port = 9092 },
}


local cli = client:new(broker_list)
local brokers, partitions = cli:fetch_metadata("test")

if not brokers then
  ngx.say("fetch_metadata failed, err:", partitions)
end
ngx.say("brokers: ", cjson.encode(brokers), "; partitions: ", cjson.encode(partitions))
