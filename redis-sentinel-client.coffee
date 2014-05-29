RedisSingleClient = require('gf-redis')
events = require('events')
util = require('util')
reply_to_object = require('gf-redis/lib/util.js').reply_to_object
to_array = require('gf-redis/lib/to_array.js')
commands = require('gf-redis/lib/commands')


class RollArray
    constructor:(arr)->
        @setData arr
        
    setData:(arr)->
        @arr=[]
        if arr
            for item in arr
                @arr.push item
        return @

    next:()->
        if @arr.length==0 then return null
        item=@arr.shift()
        @arr.push item
        return item

class LoggerWrap
    constructor:(@logger)->
    debug:(obj)=>
        if @logger and @logger.debug
            @logger.debug obj
    info:(obj)=>
        if @logger and @logger.info
            @logger.info obj
    warn:(obj)=>
        if @logger and @logger.warn
            @logger.warn obj
    error:(obj)=>
        if @logger and @logger.error
            @logger.error obj
    fetal:(obj)=>
        if @logger and @logger.fetal
            @logger.fetal obj

class Client extends events.EventEmitter
    constructor:(@name,@role,@options)->
        @id="#{@name}-#{@role}"
        @client=null
        @logger=@options.logger
        @status="disconnect"    #connected,connecting
        @timeoutClock=null
        @pingClock=null
        @firstConnect=true
        #订阅的集合
        @subSet=[]

    disconnect:()=>
        if @client
            @logger.info "#{@id} disconnect #{@host}:#{@port}"
            @client.end()
            @client=null
            if @status=="connected"
                @emit "end"
        @host=null
        @port=null
        @status="disconnect"
        ###
        if @pingClock
            clearInterval @pingClock
            @pingClock=null
        ###

    getStatus:()=>
        return @status
    getId:()=>
        return @id
    getName:()=>
        return @name
    getRole:()=>
        return @role
    getHost:()=>
        return @host
    getPort:()=>
        return @port

    isSameServer:(host,port)=>
        return @host==host and @port==port

    connect:(host,port)=>
        if @host==host and @port==port and @status!="disconnect"
            #正在连接或者已经连接上，返回
            @logger.info "#{@id} is #{@status} #{host}:#{port},return"
            return
        #先断开
        @disconnect()

        @host=host
        @port=port
        @logger.info "#{@id} try to connect #{@host}:#{@port}"
        @client=new RedisSingleClient.createClient @port,@host,@options
        @status="connecting"
        @emit "connecting"
        @client.on "connect",@onClientConnect.bind(@)
        @client.on "error",@onClientError.bind(@)
        @client.on "end",@onClientEnd.bind(@)
        @client.on "reconnecting",@onClientReconnecting.bind(@)
        @errorTimes=0
        client=@client
        self=@
        #ignore error 'connect',
        ['message', 'pmessage', 'unsubscribe', 'end', 'reconnecting',  'ready', 'subscribe'].forEach (evt)->
            client.on evt,()->
                if self.client==client
                    self.emit.apply self,[evt].concat(Array.prototype.slice.call(arguments))

    onClientConnect:()=>
        @status="connected"
        @logger.info "#{@id} success connect #{@host}:#{@port}"
        @errorTimes=0
        @emit "connect",@id
        if @firstConnect
            @emit "firstconnect",@id
            @firstConnect=false
        else
            @emit "reconnect",@id

        ###
        if not @pingClock
            self=@
            pingFun=()->
                self.client.ping (err,d)->
                    if err
                        self.logger.error "client #{self.id} ping #{self.host}:#{self.port} error:#{err}"
            @pingClock=setInterval pingFun,@options.pingTime
        ###

        #reset sub
        for sub in @subSet
            @client[sub.action].call @client,sub.topic
        
    onClientEnd:()=>
        @status="disconnect"
        @logger.info "#{@id} disconnect #{@host}:#{@port}"
        @errorTimes=0

        ###
        if @pingClock
            clearInterval @pingClock
            @pingClock=null
        ###
        @emit "end"

    onClientReconnecting:()=>
        @status="connecting"
        @logger.info "#{@id} reconnecting #{@host}:#{@port}"
        @emit "reconnecting"
 

    onClientError:(error)=>
        @logger.error "#{@id} #{@host}:#{@port} error:#{error}"
        @errorTimes++
        if @errorTimes>3
            @disconnect()
            @emit "reconnectClient",@name
            @errorTimes=0

    timeoutReconnect:()->
        if @timeoutClock
            return
        self=@
        fun=()->
            @timeoutClock=null
            self.emit "reconnectClient",self.name
        @timeoutClock=setTimeout fun,self.options.noSlaveReconnectTime
        

commands.forEach (command)->
    Client.prototype[command.toUpperCase()] = Client.prototype[command]=(args,callback)->
        if @status!="connected"
            len=arguments.length
            cb=arguments[len-1]
            if typeof cb=="function"
                cb "client is not connected,#{@status}"
            return
            
        fn=@client[command]
        fn.apply @client,arguments

Client.prototype._sub=(action,unaction,topics)->
    @logger.info "sub action=#{action},unaction=#{unaction},topics=#{topics}"
    if topics==null or topics.length==0 then return
    addTopics=[]
    for topic in topics
        find=false
        for sub in @subSet
            if sub.action==action and sub.topic==topic
                find=true
                break
        if not find
            #add topic
            @subSet.push {action:action,unaction:unaction,topic:topic}
            addTopics.push topic
    if @status=="connected"
        @client[action].apply @client,addTopics

Client.prototype._unsub=(action,topics)->
    @logger.info "unsub action=#{action},topics=#{topics}"
    if topics==null or topics.length==0 then return
    delTopics=[]
    for topic in topics
        i=0
        for sub in @subSet
            if sub.unaction==action and sub.topic==topic
                @subSet.splice i,1
                delTopics.push topic
                break
            i++
    if @status=="connected"
        @client[action].apply @client,delTopics

Client.prototype['subscribe']=(topic)->
    args=to_array(arguments)
    @_sub 'subscribe','unsubscribe',args
Client.prototype['psubscribe']=(topic)->
    args=to_array(arguments)
    @_sub 'psubscribe','punsubscribe',args

Client.prototype['unsubscribe']=(topic)->
    args=to_array(arguments)
    @_unsub 'unsubscribe',args
Client.prototype['punsubscribe']=(topic)->
    args=to_array(arguments)
    @_unsub 'punsubscribe',args


###
{
    clients:[
        {
            "role":"master",   //slave
            "name":"master86"
        }
    ],
    sentinels:[
        {
            "host":"x.x.x.x",
            "port":5678
        }
    ],
    logger:
}
###
class RedisSentinelClient extends events.EventEmitter
    constructor:(options)->
        @logger=new LoggerWrap options.logger
        @talkSentinel=null
        @talkSentinelStatus="disconnect"
        @subSentinel=null
        @subSentinelStatus="disconnect"
        if options.master_debug
             RedisSingleClient.debug_mode = true

        @talkSentinelPingTime=options.talkSentinelPingtime||2000
        @checkClientTime=options.checkClientTime||20000
        @noMasterPartnerSentinelReconnectTime=options.noMasterPartnerSentinelReconnectTime||3000
        #init client
        @clients={}
        cOptions=
            logger:@logger
            pingTime:options.clientPingTime||2000
            noSlaveReconnectTime:options.clientNoSlaveReconnectTime||3000
        for c in options.clients
            client=new Client(c.name,c.role,cOptions)
            id=client.getId()
            @logger.info "create client,id=#{id}"
            cs=@clients[c.name]
            if not cs
                cs=[]
                @clients[c.name]=cs
            cs.push client

            client.on "reconnectClient",@_reconnectClient.bind(@)

        #init sentinels
        @sentinels=new RollArray()
        arr=[]
        for s in options.sentinels
            arr.push {host:s.host,port:s.port}
        @sentinels.setData arr

        @reconnectSentinel()

        @on('subSentinel disconnected', @reconnectSentinel.bind(@))
        @on('talkSentinel disconnected', @reconnectSentinel.bind(@))
        @on('change sentinel', @reconnectSentinel.bind(@))

        @on('failover start', @_disconnectClient.bind(@))
        @on('switch master', @_reconnectClient.bind(@))
        @on('change slave', @_reconnectClient.bind(@))
        @on('check sentinel', @reconnectSentinel.bind(@))

    reconnectSentinel:()=>
        @logger.info "reconnectSentinel"
        if @talkSentinel
            @talkSentinel.end()
            @talkSentinel=null
        @talkSentinelStatus="disconnect"
        if @subSentinel
            @subSentinel.end()
            @subSentinel=null
        @subSentinelStatus="disconnect"

        sentinel=@sentinels.next()
        @logger.info "use sentinel #{sentinel.host}:#{sentinel.port}"
        @_connectSentinel sentinel

    _connectSentinel:(sentinel)=>
        host=sentinel.host;port=sentinel.port
        self=@
        logger=@logger
        logger.info "_connectSentinel #{host}:#{port}"

        logger.info "talkSentinel try to connect to sentinel #{host}:#{port}"
        @talkSentinel = new RedisSingleClient.createClient(port, host)
        @talkSentinelStatus="connecting"
        @talkSentinel.on 'connect',()->
            logger.info "talkSentinel success connect sentinel #{host}:#{port}"
            self.talkSentinelStatus="connected"
            self.emit 'talkSentinel connected',sentinel
            self._checkClientHostAndPort()

            if not self.talkSentinelPingClock
                pingFun=()->
                    self.talkSentinel.ping (err)->
                        if err
                            logger.error "talkSentinel ping #{host}:#{port} error:#{err}"

                self.talkSentinelPingClock=setInterval pingFun,self.talkSentinelPingTime
            if not self.checkClientJobClock
                fun=self._checkClientJob.bind self
                self.checkClientJobClock=setInterval fun,self.checkClientTime

        @talkSentinel.on 'error',(err)->
            logger.error "talkSentinel error:#{err.message} at #{host}:#{port}"
            self.emit 'talkSentinel error', err
        @talkSentinel.on 'end',()->
            logger.error "talkSentinel end at #{host}:#{port}"
            self.talkSentinelStatus="disconnect"
            self.emit 'talkSentinel disconnected'

            if self.talkSentinelPingClock
                clearInterval self.talkSentinelPingClock
                self.talkSentinelPingClock=null

            if self.checkClientJobClock
                clearInterval self.checkClientJobClock
                self.checkClientJobClock=null

        logger.info "subSentinel try to connect to sentinel #{host}:#{port}"
        @subSentinel = new RedisSingleClient.createClient(port, host)
        @subSentinelStatus="connecting"
        @subSentinel.on 'connect',()->
            logger.info "subSentinel success connect sentinel #{host}:#{port}"
            self.subSentinelStatus="connected"
            self.emit 'subSentinel connected',sentinel
            self._checkClientHostAndPort()
        @subSentinel.on 'error',(err)->
            logger.error "subSentinel error:#{err.message} at #{host}:#{port}"
            self.emit 'subSentinel error', err
        @subSentinel.on 'end',()->
            logger.error "subSentinel end at #{host}:#{port}"
            self.subSentinelStatus="disconnect"
            self.emit 'subSentinel disconnected'

        @subSentinel.psubscribe "*"
        @subSentinel.on "pmessage",(channel,msg,args)->
            logger.info "sentinel message:channel=#{channel},msg=#{msg},args=#{args}"
            self.emit "sentinel message",msg
            if msg=="+try-failover"
                masterName=args.split(" ")[1]
                logger.info "failover detected for #{masterName}"
                self.emit "failover start",masterName
            else if msg== "+switch-master"
                masterName=args.split(" ")[0]
                logger.info "switch master detected for #{masterName}"
                self.emit "failover end",masterName
                self.emit 'switch master',masterName
            else if msg== "+slave"
                masterName=args.split(" ")[5]
                logger.info "add slave detected for #{masterName}"
                self.emit 'change slave',masterName
            else if msg=="+sdown" || msg=="-sdown"
                arr=args.split(" ")
                roleName=arr[0];masterName=""
                if roleName=="slave"
                    masterName=arr[5]
                else if roleNmae="master"
                    masterName=arr[1]
                logger.info "#{msg} detected for #{masterName},#{roleName}"
                if roleName=="slave"
                    self.emit 'change slave',masterName
                else if roleName=="sentinel"
                    #检查sentinel
                    self.emit 'check sentinel'
            else if msg=="+role-change" || msg=="-role-change"
                arr=args.split(" ")
                roleName=arr[0];masterName=""
                masterName=arr[5]
                logger.info "#{msg} detected for #{masterName},#{roleName}"
                if roleName=="slave"
                    self.emit 'change slave',masterName

        #获取
    _checkClientHostAndPort:()=>
        if not @firstCheckClientHostAndPort
            @firstCheckClientHostAndPort=true
            @emit "connect"

        logger=@logger
        logger.info "_checkClientHostAndPort,subSentinelStatus=#{@subSentinelStatus},talkSentinelStatus=#{@talkSentinelStatus}"
        if @subSentinelStatus!="connected"
            logger.info "subSentinel status is not connected,status=#{@subSentinelStatus}"
            return
        if @talkSentinelStatus!="connected"
            logger.info "talkSentinel status is not connected,status=#{@talkSentinelStatus}"
            return
        logger.info "begin checkClientHostAndPort"
        for k,v of @clients
            @_reconnectClient k

    _checkClientJob:()->
        @logger.debug "_checkClientJob start"
        if @subSentinelStatus!="connected"
            return
        if @talkSentinelStatus!="connected"
            return
        for k,v of @clients
            @_reconnectClient k,true

    getClient:(master,role="master")=>
        clients=@clients[master]
        if not clients or clients.length==0
            return null
        else
            for client in clients
                if client.getName()==master and client.getRole()==role
                    return client

            return null

    _disconnectClient:(master)=>
        logger=@logger
        clients=@clients[master]
        if not clients or clients.length==0
            logger.info "clients is empty"
            return

        clients.forEach (client)->
            client.disconnect()

    _reconnectClient:(master,checkOnly=false)=>
        logger=@logger
        if checkOnly
            logger.debug "checkClient #{master}"
        else
            logger.info "_reconnectClient:#{master}"
        if @talkSentinelStatus!="connected"
            logger.warn "talkSentinel not connected,return "
            return
        talkSentinel=@talkSentinel
        clients=@clients[master]
        if not clients or clients.length==0
            logger.info "clients is empty"
            return

        self=@
        isReconnectSentinel=false
        clients.forEach (client)->
            masterName=client.getName()
            role=client.getRole()
            if role=="master"
                talkSentinel.send_command "SENTINEL",["get-master-addr-by-name",masterName],(err,arr)->
                    if err
                        logger.error "error when get-master-addr-by-name for #{masterName}:#{err}"
                    else
                        try
                            host=arr[0]
                            port=arr[1]
                            if not host or not port
                                logger.error "no master for #{masterName},disconnect,reply=#{arr}"
                                client.disconnect()
                                client.timeoutReconnect()
                                return
                            if not (checkOnly and client.isSameServer host,port)
                                client.connect host,port
                            #检查sentinel 
                            talkSentinel.send_command "SENTINEL",['sentinels',masterName],(err,arr)->
                                if err
                                    logger.error "error when get sentinels for #{masterName}:#{err}"
                                else
                                    if arr==null || arr.length==0
                                        logger.warn "sentinel for #{masterName} is just one"
                                    else
                                        findSentinel=false
                                        for aa in arr
                                            i=0
                                            server={}
                                            while i<aa.length
                                                server[aa[i]]=aa[i+1]
                                                i+=2
                                            if server['flags']=='sentinel'
                                                findSentinel=true
                                                break
                                        if findSentinel==false
                                            logger.error "sentinel for #{masterName} no partner sentinel is ok"

                                            if isReconnectSentinel==false
                                                isReconnectSentinel=true
                                                fun=()->
                                                    self.emit "change sentinel"
                                                setTimeout fun,self.noMasterPartnerSentinelReconnectTime
                        catch e
                            logger.error "unable get master for #{masterName},reply=#{arr},err=#{e},#{e.stack}"
            else if role=="slave"
                talkSentinel.send_command "SENTINEL",["slaves",masterName],(err,arr)->
                    if err
                        logger.error "error when slaves for #{masterName}:#{err}"
                    else
                        try
                            if arr==null || arr.length==0
                                logger.error "no slave for #{masterName},disconnect"
                                client.disconnect()
                                client.timeoutReconnect()
                            else
                                servers=[]
                                for aa in arr
                                    i=0
                                    server={}
                                    while i<aa.length
                                        server[aa[i]]=aa[i+1]
                                        i+=2
                                    if server['flags']=='slave'
                                        servers.push server
                                if servers.length==0
                                    logger.error "no slave status is ok for #{masterName},disconnect"
                                    client.disconnect()
                                    client.timeoutReconnect()
                                    return
                                if checkOnly
                                    #只检查
                                    for server in servers
                                        if client.isSameServer server['ip'],server['port']
                                            return
                                    #not find

                                index=Math.floor Math.random()*servers.length
                                slave=servers[index]
                                host=slave['ip']
                                port=slave['port']
                                client.connect host,port
                        catch e
                            logger.error "unable get slave for #{masterName},reply=#{arr},err=#{e},#{e.stack}"
            else
                throw new Error("unknow role for master=#{masterName},role=#{role}")


        
module.exports.RedisSentinelClient=RedisSentinelClient
module.exports.createClient=(options)->
    return new RedisSentinelClient(options)
