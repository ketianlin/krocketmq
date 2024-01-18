package model

type TopicMessage struct {
	Msg         string   `json:"msg"`
	TopicName   string   `json:"topicName"`
	Tags        string   `json:"tags"`
	Keys        []string `json:"keys"`
	ShardingKey string   `json:"shardingKey"`
}

type InitCallbackMessage struct {
	Version      string // 当前版本号
	IsSuccessful bool   // 是否初始化成功
	InitError    error  // 初始化错误信息
}
