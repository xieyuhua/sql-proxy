package main

import (
	"math/rand"
	"net"
	"time"

	"stathat.com/c/consistent"
)

// 代理服务的结构
type BackendSvr struct {
	identify  string
	isLive    bool // 服务是否存活
	failTimes int
}

var (
	Consisthash *consistent.Consistent
	BackendSvrs map[string]*BackendSvr
)

// 初始化代理服务，加入一致性哈希列表，开始检测服务
func initBackendSvrs(serverList []string) {
	Consisthash = consistent.New()
	BackendSvrs = make(map[string]*BackendSvr)
	for _, server := range serverList {
		Consisthash.Add(server)
		BackendSvrs[server] = &BackendSvr{
			identify:  server,
			isLive:    true,
			failTimes: 0,
		}
	}
	go checkBackendSvrs()
}

// 根据客户端链接，从哈希集群中选择一台机器
func getBackendSvr(conn net.Conn) (*BackendSvr, bool) {
	remoteAddr := conn.RemoteAddr().String()
	identify, _ := Consisthash.Get(remoteAddr)
	BackendSvr, ok := BackendSvrs[identify]
	return BackendSvr, ok
}

func removeElement(slice []string, elem string) []string {
    for i, v := range slice {
        if v == elem {
            return append(slice[:i], slice[i+1:]...)
        }
    }
    return slice
}


var RemoveList []string
// 代理服务检测存活
func checkBackendSvrs() {
    
	//重新检查服务
	for _, item := range BackendSvrs {
	    //初始化代理服务
    	_, err := net.Dial("tcp", item.identify)
    	if err != nil {
			item.isLive = false
			println("Remove " , item.identify)
			Consisthash.Remove(item.identify)
			RemoveList = append(RemoveList, item.identify)
    	}
	}
	
	rand.Seed(time.Now().UnixNano())
	// 设置定时（10s对服务进行检测）执行管道
	ticker := time.Tick(time.Duration(5) * time.Second)
	for _ = range ticker {
		for _, server := range BackendSvrs {
			if server.failTimes >= Config.FailOver && server.isLive == true {
				server.isLive = false
				println("Remove ", server.identify)
				Consisthash.Remove(server.identify)
				RemoveList = append(RemoveList, server.identify)
			}
		}
		//重新检查服务
    	for _, serfail := range RemoveList {
        	// 链接远程代理服务器
        	_, err := net.Dial("tcp", serfail)
        	if err == nil {
            	RemoveList = removeElement(RemoveList, serfail)
        		Consisthash.Add(serfail)
        		BackendSvrs[serfail] = &BackendSvr{
        			identify:  serfail,
        			isLive:    true,
        			failTimes: 0,
        		}
        	}
    	}
	}
}




// 设置定时器
func timer(input chan interface{}) {
	timerOne := time.NewTimer(time.Second * 5)
	timerTwo := time.NewTimer(time.Second * 10)
	for {
		select {
		case msg := <-input:
			println(msg)

		case <-timerOne.C:
			println("5s timer")
			timerOne.Reset(time.Second * 5)

		case <-timerTwo.C:
			println("10s timer")
			timerTwo.Reset(time.Second * 10)
		}
	}
}
