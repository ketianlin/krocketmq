package model

type Config struct {
	NameServers    []string
	ProductConfig  ProductConfig
	ConsumerConfig ConsumerConfig
}

type ProductConfig struct {
	RetryCount     int
	Timeout        int
	TopicQueueNums int
	Group          string
	LogLevel       string // 日志级别: debug, warn, error, fatal, info(默认)
}

type ConsumerConfig struct {
	Timeout        int
	Group          string
	MonitoringTime int    // 监测时间 单位（秒）
	LogLevel       string // 日志级别: debug, warn, error, fatal, info(默认)
}
