package main

import (
	"flag"
	"fmt"
	"net/http"
	"log"
	"os"
	"gopkg.in/natefinch/lumberjack.v2"
	"os/signal"
	"syscall"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"github.com/go-yaml/yaml"
)

var (
	Config     ProxyConfig
	Log        *logrus.Logger
	configFile = flag.String("c", "./conf.yaml", "配置文件：conf.yaml")
	Logs       = logrus.New()
)

// 代理配置数据结构
type ProxyConfig struct {
	Bind         string    `yaml:"bind"`
	Type         string    `yaml:"type"`
	WaitQueueLen int       `yaml:"wait_queue_len"`
	MaxConn      int       `yaml:"max_conn"`
	BulkSize      int       `yaml:"bulk_size"`
	Timeout      int       `yaml:"timeout"`
	FailOver     int       `yaml:"failover"`
	SlowTime     int       `yaml:"slow_time"`
	Backend      []string  `yaml:"backend"`
	Log          LogConfig `yaml:"log"`
	Stats        string    `yaml:"stats"`
}

// 日志配置结构信息
type LogConfig struct {
	Level string `yaml:"level"`
	Path  string `yaml:"path"`
}

// 解析配置文件
func parseConfigFile(filePath string) error {
	if conf, err := ioutil.ReadFile(filePath); err == nil {
		if err = yaml.Unmarshal(conf, &Config); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

func onExitSignal() {
	signalChan := make(chan os.Signal)
	// 监听系统服务退出信号
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGTERM, syscall.SIGINT, os.Kill)
	for {
		signal := <-signalChan
		log.Println("Get Signal:%v\r\n", signal)
		switch signal {
		case syscall.SIGTERM, syscall.SIGINT, os.Kill:
			log.Fatal("系统退出。。。")
		}
	}
}

// 初始化日志模块
func initLogger() error {
	logFilePath := Config.Log.Path
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// 解析日志记录的等级信息
	level, err := logrus.ParseLevel(Config.Log.Level)
	if err != nil {
		return err
	}
	// 初始化日志结构
	Log = &logrus.Logger{
		Out:       file,
		Level:     level,
		Formatter: new(logrus.JSONFormatter),
	}
	
	
    //日志
    Logs.SetFormatter(&logrus.JSONFormatter{})
    
	logger := &lumberjack.Logger{
		Filename:   "logs/logrus.log",
		MaxSize:    50,  // 日志文件大小，单位是 MB
		MaxBackups: 3,    // 最大过期日志保留个数
		MaxAge:     30,   // 保留过期文件最大时间，单位 天
		Compress:   true, // 是否压缩日志，默认是不压缩。这里设置为true，压缩日志
	}
	Logs.SetOutput(logger) // logrus 设置日志的输出方式
	
	
	return nil
}

// 查询监控信息的接口
func statsHandler(w http.ResponseWriter, r *http.Request) {
	_str := ""
	_str += fmt.Sprintf("Server connecting num:%d \n\n", onlineNum)
	for _, v := range BackendSvrs {
		_str += fmt.Sprintf("Server:%s FailTimes:%d isUp:%t\n", v.identify, v.failTimes, v.isLive)
	}
// 	w.Write([]byte(_str))
	
	fmt.Fprintf(w, "%s", _str)
	
}

// 初始化监控服务地址
func initStats() {
	Log.Infof("Start monitor on addr %s", Config.Stats)

	go func() {
		http.HandleFunc("/", statsHandler)
		http.ListenAndServe(Config.Stats, nil)
	}()
}


func main() {

	flag.Parse()
	
	// 解析配置
	parseConfigFile(*configFile)
    fmt.Println("parseConfig finish...")
	// 初始化日志模块
	initLogger()
    fmt.Println("Logger finish...")
	// 初始化代理的服务
	initBackendSvrs(Config.Backend)
    fmt.Println("Proxy finish...")
	// 系统退出信号监听
	go onExitSignal()

	// 初始化状态服务
	initStats()
	fmt.Printf("Listen server: %v \n", Config.Bind)
    fmt.Println("Start successful \n\n\n")
    
	// 初始化代理服务
	initProxy()
    
}
