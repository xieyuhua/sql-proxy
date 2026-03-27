##go-upstream
服务代理中间件，支持后端服务集群配置，根据HASH配置选择后端机器进行代理。

##安装方法
```
go get github.com/fbbin/go-upstream
```

##Examples

```
bind: 0.0.0.0:9800
wait_queue_len: 100
max_conn: 50
timeout: 5 //连接时间长，自动断开
failover: 3 //连接失败，重试次数
stats: 0.0.0.0:8090
backend:
    - 192.168.163.184:3306
    - 192.168.163.184:3306
    - 192.168.163.184:3306

log:
    level: "info"
    path: "./logs/proxy.log"
```
## swoole and proxy swoole

```
[root@iZ2vc4fcja0fjd7ljf2a9cZ httpstatus]# ./httpstatus http://47.xx.xx.35

Connected to 47.xx.xx.35:80

Connected via plaintext

HTTP/1.1 200 OK
Server: nginx
Content-Type: text/html; charset=utf-8
Date: Thu, 07 Jul 2022 11:25:24 GMT
Vary: Accept-Encoding
Connection: keep-alive

Body discarded

   DNS Lookup   TCP Connection   Server Processing   Content Transfer
[       0ms  |           0ms  |              8ms  |             0ms  ]
             |                |                   |                  |
    namelookup:0ms            |                   |                  |
                        connect:0ms               |                  |
                                      starttransfer:9ms              |
                                                                 total:9ms      
[root@iZ2vc4fcja0fjd7ljf2a9cZ httpstatus]# 
[root@iZ2vc4fcja0fjd7ljf2a9cZ httpstatus]# ./httpstatus http://47.xx.xx.35:9810

Connected to 47.xxx.xx.35:9810

Connected via plaintext

HTTP/1.1 200 OK
Server: nginx
Content-Type: text/html; charset=utf-8
Date: Thu, 07 Jul 2022 11:25:25 GMT
Vary: Accept-Encoding
Connection: keep-alive

Body discarded

   DNS Lookup   TCP Connection   Server Processing   Content Transfer
[       0ms  |           0ms  |              9ms  |             0ms  ]
             |                |                   |                  |
    namelookup:0ms            |                   |                  |
                        connect:0ms               |                  |
                                      starttransfer:10ms             |
                                                                 total:10ms     

```


```
for {
	select {
	case <-ctx.Done():
		return
	default:
	}

	// 1. Read
	n, err := from.Read(buf)
	if err != nil {
		return
	}

	// 2. Append
	tns.Append(buf[:n])

	// 3. Drain all packets
	for {
		pkt, ok := tns.NextPacket()
		if !ok {
			break
		}
		handlePacket(pkt)
	}
}
```

