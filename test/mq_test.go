package test

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/config"
	"github.com/ketianlin/kgin/logs"
	"github.com/ketianlin/krocketmq/consumer"
	"github.com/ketianlin/krocketmq/model"
	"github.com/ketianlin/krocketmq/producer"
	"runtime/debug"
	"testing"
)

const TopicName = "Queue_809_FuQing_Up"

func TestClose(t *testing.T) {
	producer.ProducerClient.Close()
}

func TestRocketMq(t *testing.T) {
	configFile := "/home/ke666/my_codes/go_codes/krocketmq/test/krocketmq.yml"
	fmt.Println(configFile)
	config.Config.Init(configFile)
	//kgin.KGin.Use("rocketmq", producer.ProducerClient.Init, producer.ProducerClient.Close, nil)
	kgin.KGin.Use("rocketmq", consumer.ConsumerClient.Init, consumer.ConsumerClient.Close, consumer.ConsumerClient.MqCheck)
	//go ListenRMQ()
	go ListenRMQNew()
	//SendMessage()
	//producer.ProducerClient.Close()
	//producer.ProducerClient.Close()
	select {}
}

func TestRocketMqByConfig(t *testing.T) {
	mc := model.Config{
		NameServers: []string{"192.168.20.135:9876"},
		ProductConfig: model.ProductConfig{
			RetryCount:     2,
			TopicQueueNums: 16,
			Timeout:        5,
			Group:          "sjProductGroup",
			LogLevel:       "error",
		},
		ConsumerConfig: model.ConsumerConfig{
			Timeout:        5,
			Group:          "sjConsumerGroup33",
			MonitoringTime: 10,
			LogLevel:       "error",
		},
	}
	//producer.ProducerClient.InitConfig(&mc, func(err error) {
	//	fmt.Println("err: ", err)
	//})
	// 关闭生产者判断
	//producer.ProducerClient.Close()
	//if err := producer.ProducerClient.GetCloseError(); err != nil {
	//	fmt.Println("关闭生产者失败：", err.Error())
	//	return
	//}
	consumer.ConsumerClient.InitConfig(&mc, func(im *model.InitCallbackMessage) {
		fmt.Println("im: ", im)
	})
	//// 关闭消费者判断
	//consumer.ConsumerClient.Close()
	//if err := consumer.ConsumerClient.GetCloseError(); err != nil {
	//	fmt.Println("关闭消费者失败：", err.Error())
	//	return
	//}
	go ListenRMQ()
	//SendMessage()
	select {}
}

type testEventHandler struct{}

var Test testEventHandler

func (o *testEventHandler) handleExpireFullMsg(msg *primitive.MessageExt) {
	logs.Debug("接收到原始消息 {},-------{}", msg.MsgId, string(msg.Body))
	//safeHandler(msg, func(msg []byte) {
	//	logs.Info("打印序列化后消息：{}", string(msg))
	//})
}

func (o *testEventHandler) handleExpireMsg(msg []byte) {
	logs.Debug("接收到原始消息 {}", string(msg))
	safeHandler(msg, func(msg []byte) {
		logs.Info("打印序列化后消息：{}", string(msg))
	})
}

func (o *testEventHandler) handleExpireMsgNew(topicName string, msg []byte) {
	//logs.Debug("topic: 【{}】, 接收到原始消息 {}", topicName, string(msg))
	logs.Info("{}", string(msg))
	//safeHandler(msg, func(msg []byte) {
	//	//logs.Info("topic: 【{}】, 打印序列化后消息：{}", topicName, string(msg))
	//	logs.Debug(string(msg))
	//})
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

func ListenRMQNew() {
	logs.Debug("rocketmq客户端 获取连接成功")
	consumer.ConsumerClient.MessageListenerNew(TopicName, Test.handleExpireMsgNew, func(err error) {
		logs.Debug("MessageListener error: ", err.Error())
	})
}

func ListenRMQ() {
	logs.Debug("rocketmq客户端 获取连接成功")
	//consumer.ConsumerClient.MessageListener(TopicName, Test.handleExpireMsg, func(err error) {
	//	fmt.Println("MessageListener error: ", err.Error())
	//})
	consumer.ConsumerClient.MessageListenerReturnFullMessage(TopicName, Test.handleExpireFullMsg, func(err error) {
		fmt.Println("MessageListener error: ", err.Error())
	})
}

func SendMessage() {
	logs.Debug("rocketmq生产者发消息拉……")
	for i := 0; i < 5; i++ {
		mt := &model.TopicMessage{
			Msg:       fmt.Sprintf("吊毛来了99999999999999-%d", i),
			TopicName: TopicName,
			Tags:      "sj-tag",
			Keys:      []string{"sj-key"},
		}
		mt.ShardingKey = "d9999"
		err := producer.ProducerClient.SendSync(mt)
		if err != nil {
			logs.Error("rocketmq发送消息失败")
			return
		}
		logs.Info("发送成功")
	}
}
