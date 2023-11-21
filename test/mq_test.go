package test

import (
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/config"
	"github.com/ketianlin/kgin/logs"
	"github.com/ketianlin/krocketmq/consumer"
	"github.com/ketianlin/krocketmq/model"
	"github.com/ketianlin/krocketmq/producer"
	"runtime/debug"
	"testing"
)

const TopicName = "sj18"

func TestRocketMq(t *testing.T) {
	configFile := "/home/ke666/my_codes/go_codes/krocketmq/test/krocketmq.yml"
	fmt.Println(configFile)
	config.Config.Init(configFile)
	kgin.KGin.Use("rocketmq", producer.ProducerClient.Init, producer.ProducerClient.Close, nil)
	kgin.KGin.Use("rocketmq", consumer.ConsumerClient.Init, consumer.ConsumerClient.Close, nil)
	go ListenRMQ()
	SendMessage()
	select {}
}

type testEventHandler struct{}

var Test testEventHandler

func (o *testEventHandler) handleExpireMsg(msg []byte) {
	logs.Debug("接收到原始消息 {}", string(msg))
	safeHandler(msg, func(msg []byte) {
		logs.Info("打印序列化后消息：{}", string(msg))
	})
}

func safeHandler(msg []byte, handler func(msg []byte)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logs.Error("goroutine process panic {} stack: {}", err, string(debug.Stack()))
			}
		}()
		handler(msg)
	}()
}

func ListenRMQ() {
	logs.Debug("rocketmq客户端 获取连接成功")
	consumer.ConsumerClient.MessageListener(TopicName, Test.handleExpireMsg)
}

func SendMessage() {
	logs.Debug("rocketmq生产者发消息拉……")
	err := producer.ProducerClient.SendSync(&model.TopicMessage{
		Msg:       "吊毛来了",
		TopicName: TopicName,
		Tags:      "sj-tag",
		Keys:      []string{"sj-key"},
	})
	if err != nil {
		logs.Error("rocketmq发送消息失败")
		return
	}
	logs.Info("发送成功")
}
