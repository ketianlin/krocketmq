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
	InitError    error  // 初始化错误信息
	MqCheckError error  // 检查mq是否重连错误信息
}
