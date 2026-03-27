package main

import (
	"net"
	"time"
	"fmt"
	"sync"
	"context"
	"io"
	"sync/atomic"
	"tcpproxy/protocol"
    "github.com/sirupsen/logrus"
)

var onlineNum int64

var dataType = map[string]bool{
	"mysql":  true,
	"redis": true,
	"oracle": true,
	"http": true,
}

func inc() { atomic.AddInt64(&onlineNum, 1) }
func dec() { atomic.AddInt64(&onlineNum, -1) }

var sqlChan = make(chan string, 1000)

func init() {
	go func() {
		for sql := range sqlChan {
			fmt.Println(sql)
		}
	}()
}

// ------------------ Slow SQL ------------------
func logSlowSQL(sql string, cost float64) {
	if cost <= float64(Config.SlowTime) || sql == "" {
		return
	}
	select {
        case sqlChan <- sql:
        default:
    }
	Logs.WithFields(logrus.Fields{
		"cost_ms": fmt.Sprintf("%.2f", cost),
		"sql":     sql,
	}).Warn("slow sql")
	
}


// 初始化代理服务
func initProxy() {
	Log.Infof("Proxying %s -> %s \n", Config.Bind, Config.Backend)
	server, err := net.Listen("tcp", Config.Bind)
	if err != nil {
		Log.Fatal(err)
	}
	
	queue := make(chan net.Conn, Config.WaitQueueLen)
	for i := 0; i < Config.MaxConn; i++ {
		go worker(queue)
	}
	
	// 接收连接并抛给管道处理
	for {
		conn, err := server.Accept()
		if err != nil {
			Log.Error(err)
			continue
		}
		Log.Infof("Received connection from %s.\n", conn.RemoteAddr())
		queue <- conn
	}
}

func worker(queue <-chan net.Conn) {
	for conn := range queue {
		handleConn(conn)
	}
}

// ------------------ Session ------------------
type Session struct {
	StartTime int64
	SQL       string
}

const sessionMapShards = 64 // 2的幂，方便位运算取模

// ShardedSessionManager 代表分片的会话管理器
type ShardedSessionManager struct {
    shards [sessionMapShards]struct {
        mu   sync.RWMutex
        data map[string]*Session
    }
}

// 全局单例
var globalSessionMgr = newShardedSessionManager()

func newShardedSessionManager() *ShardedSessionManager {
    mgr := &ShardedSessionManager{}
    for i := range mgr.shards {
        mgr.shards[i].data = make(map[string]*Session)
    }
    return mgr
}

// getShard 根据 key 获取对应的分片
func (m *ShardedSessionManager) getShard(key string) *struct {
    mu   sync.RWMutex
    data map[string]*Session
} {
    // 使用 FNV 哈希或其他快速哈希算法
    hash := fnv1aHash(key)
    return &m.shards[hash%sessionMapShards]
}

func fnv1aHash(s string) uint32 {
    const (
        offset32 = 2166136261
        prime32  = 16777619
    )
    h := uint32(offset32)
    for i := 0; i < len(s); i++ {
        h ^= uint32(s[i])
        h *= prime32
    }
    return h
}

// StoreSession 存储会话
func (m *ShardedSessionManager) StoreSession(id string, s *Session) {
    shard := m.getShard(id)
    shard.mu.Lock()
    shard.data[id] = s
    shard.mu.Unlock()
}

// DeleteSession 删除会话并返回
func (m *ShardedSessionManager) DeleteSession(id string) (*Session, bool) {
    shard := m.getShard(id)
    shard.mu.Lock()
    defer shard.mu.Unlock()
    s, ok := shard.data[id]
    if ok {
        delete(shard.data, id)
    }
    return s, ok
}

// --- 替换原有的全局函数 ---
func storeSession(id string, s *Session) {
    globalSessionMgr.StoreSession(id, s)
}

func deleteSession(id string) (*Session, bool) {
    return globalSessionMgr.DeleteSession(id)
}

// 但 占比 73.35% 的 CPU 时间 → 锁竞争已经成主瓶颈
// 👉 常见来源：
// sync.Mutex
// sync.WaitGroup
// channel send / recv
// sync.Map

var connIDGen uint64

func genConnID() string {
	return fmt.Sprintf("%d", atomic.AddUint64(&connIDGen, 1))
}

// ------------------ Handle Conn ------------------
func handleConn(conn net.Conn) {
    
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer conn.Close()
	
	// 根据链接哈希选择机器
	proxySvr, ok := getBackendSvr(conn)
	if !ok {
		return
	}
	// 链接远程代理服务器
	remote, err := net.Dial("tcp", proxySvr.identify)
	if err != nil {
		Log.Error(err)
		proxySvr.failTimes++
		return
	}
	defer remote.Close()
	
	
	// 将当前客户端链接发送的数据发送给远程被代理的服务器
	Log.Infof("from %s to %s.\n", conn.RemoteAddr(), remote.RemoteAddr())
	
	
	if dataType[Config.Type] {
        connID := genConnID()
        errCh := make(chan error, 2)

        go func() {
            err := transactionNew(ctx, connID, conn, remote, true)
            errCh <- err
        }()
        go func() {
            err := transactionNew(ctx, connID, remote, conn, false)
            errCh <- err
        }()

        // 等待任一方向结束
        err := <-errCh
        if err != nil && err != io.EOF {
            Log.Debugf("stream ended with error: %v", err)
        }
        // 主动取消，确保另一个 goroutine 退出，触发 defer 关闭
        cancel() 
        
        // 等待另一个 goroutine 退出（可选，防止僵尸协程，虽然 ctx 取消后它们很快会退）
        <-errCh
	}else{
        // 任一方断开即可退出 
        // 不会 goroutine 泄漏
        errCh := make(chan error, 2)
        go func() {
            _, err := io.Copy(remote, conn)
            errCh <- err
        }()
        go func() {
            _, err := io.Copy(conn, remote)
            errCh <- err
        }()
        <-errCh // 任意一个方向结束就退出
        cancel()
	}
	<-ctx.Done()
}

// 增加带超时的 Reader 包装器
type deadlineReader struct {
    conn    net.Conn
    timeout time.Duration
}

func (d *deadlineReader) Read(p []byte) (n int, err error) {
    d.conn.SetReadDeadline(time.Now().Add(d.timeout))
    return d.conn.Read(p)
}

// ------------------ Transaction ------------------
// sql 补全
func transactionNew(ctx context.Context, connID string, from, to net.Conn, out bool) error  {
   // 使用包装器处理读超时
    reader := &deadlineReader{conn: from, timeout: time.Duration(Config.Timeout) * time.Second}
    writer := NewTNSWriter(to, from, connID, out, Config.BulkSize)
    defer writer.Close()

    // 超时兜底：如果 ctx 取消，强制关闭
    // 注意：这里不需要单独开 goroutine 去 close，因为 ctx.Done() 通常由外部控制
    // 但如果想确保 io.Copy 能立即感知到关闭，可以在这里监听
    done := make(chan struct{})
    defer close(done)
    
    go func() {
        select {
        case <-ctx.Done():
            from.Close()
            to.Close()
        case <-done:
            return
        }
    }()

    _, err := io.Copy(writer, reader)
    return err
}

// ------------------ TNSWriter ------------------
type TNSWriter struct {
	to     net.Conn
	from   net.Conn
	buf    []byte
	pos    int

	connID string
	out    bool
}

var bufPool = sync.Pool{
	New: func() any {
        // 假设 Config.BulkSize 在初始化后是只读的
		size := Config.BulkSize
		if size < 32 * 1024 { // 确保最小尺寸
			size = 32 * 1024
		}
		return make([]byte, size)
	},
}

func NewTNSWriter(to, from net.Conn, connID string, out bool, size int) *TNSWriter {
	return &TNSWriter{
		to: to,
		from: from,
		buf:   bufPool.Get().([]byte),
		connID: connID,
		out:    out,
	}
}

func (w *TNSWriter) Close() {
	bufPool.Put(w.buf)
	w.buf = nil
}

func (w *TNSWriter) Write(p []byte) (int, error) {
    //写前清空 buffer
    if len(p) > len(w.buf) {
		return 0, io.ErrShortBuffer
	}
	copy(w.buf[w.pos:], p)
	w.pos += len(p)
    var pktLen int
    // var err error
	for {
	    if w.pos == 0 {
    		return len(p), nil
    	}
		if Config.Type != "oracle" {
		    pktLen = w.pos
		}else{
    		if w.pos < 8 {
    			return len(p), nil
    		}
    		pktLen = int(w.buf[0])<<8 | int(w.buf[1])
    		if pktLen < 8 || pktLen > 64 * 1024 {
    			copy(w.buf, w.buf[8:w.pos])
    			w.pos -= 8
    			continue
    		}
    		if w.pos < pktLen {
    			return len(p), nil
    		}
		}

		pkt := w.buf[:pktLen]

		// ===== OUT =====
		if w.out {
			sql := ""
			switch Config.Type {
			case "oracle":
				sql = protocol.ParseOracleSQL(w.from.RemoteAddr().String(), w.to.RemoteAddr().String(), pkt)
			case "mysql":
				sql = protocol.ParseMysqlSQL(w.from.RemoteAddr().String(), w.to.RemoteAddr().String(), pkt)
			case "redis":
				sql = string(pkt)
			}

			if sql != "" {
            	storeSession(w.connID, &Session{
            		StartTime: time.Now().UnixNano(),
            		SQL:       sql,
            	})
			}
			
		} else {
			// ===== IN =====
            if !w.out {
            	if s, ok := deleteSession(w.connID); ok {
            		cost := float64(time.Now().UnixNano()-s.StartTime) / float64(time.Millisecond)
            		logSlowSQL(s.SQL, cost)
            	}
            }
		}
		if _, err := w.to.Write(pkt); err != nil {
			return 0, err
		}

		copy(w.buf, w.buf[pktLen:w.pos])
		w.pos -= pktLen
	}
}

