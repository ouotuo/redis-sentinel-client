RedisSentinelClient = require "./redis-sentinel-client"
options=
    clients:[
        {role:"master",name:"mymaster"},
        {role:"slave",name:"mymaster"}
    ]
    sentinels:[
        {host:"10.2.126.53",port:"26379"},
        {host:"10.2.126.53",port:"26389"},
        {host:"10.2.126.53",port:"26399"}
    ]
    logger:console
    talkSentinelPingTime:2000
    clientPingTime:2000
    clientNoSlaveReconnectTime:3000
    checkClientTime:10000
    noMasterPartnerSentinelReconnectTime:5000


process.on 'uncaughtException', (err)->
    console.error((new Date).toUTCString() + ' uncaughtException:', err.message)
    console.error(err.stack)
    process.exit(1)

client=new RedisSentinelClient.createClient options
c=client.getClient "mymaster"
ping=(err)->
    if err
        console.error "ping error:#{err}"
    else
        console.info "ping success"

c.on "firstconnect",()->
    console.log "firstconnect---------------"
    lua="local arr=redis.call('zrange',KEYS[1],0,999999); 
         for i,v in ipairs(arr) do  redis.call('zadd',KEYS[1],-0.0000001/i,v) end;
         return nil;"
    c.eval lua,1,"rank:a",(err,ret)->
        if err
            console.error err
        else
            console.log "lua success"

c.on "connecting",()->
    console.log "connecting---------------"
c.on "reconnect",()->
    console.log "reconnect---------------"
c.on "connect",()->
    console.log "connect---------------"



ping()
s=client.getClient "mymaster","slave"
s.subscribe "a","c","d"
s.subscribe "b"
s.unsubscribe "b","d"
s.psubscribe "b*f"
s.on "message",(channel,msg)->
    console.log "message:#{channel},#{msg}"
s.on "pmessage",(channel,msg)->
    console.log "pmessage:#{channel},#{msg}"







###
sortedSetFindAndRemLua="local arr=redis.call('zrevrangebyscore',KEYS[1],ARGV[2],ARGV[1],'WITHSCORES');redis.call('zremrangebyscore',KEYS[1],ARGV[1],ARGV[2]);return arr;"
###

