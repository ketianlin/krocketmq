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
}

type ConsumerConfig struct {
	Timeout int
	Group   string
}
