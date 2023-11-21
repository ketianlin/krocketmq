package model

type TopicMessage struct {
	Msg       string   `json:"msg"`
	TopicName string   `json:"topicName"`
	Tags      string   `json:"tags"`
	Keys      []string `json:"keys"`
}
