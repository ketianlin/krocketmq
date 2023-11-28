package test

import (
	"fmt"
	"github.com/ketianlin/kgin"
	"github.com/ketianlin/kgin/config"
	"github.com/ketianlin/krocketmq/consumer"
	"github.com/ketianlin/krocketmq/model"
	"github.com/ketianlin/krocketmq/producer"
	"testing"
	"time"
)

func TestRocketMqByConfig3(t *testing.T) {
	configFile := "/home/ke666/my_codes/go_codes/krocketmq/test/krocketmq.yml"
	fmt.Println(configFile)
	config.Config.Init(configFile)
	kgin.KGin.Use("rocketmq", producer.ProducerClient.Init, producer.ProducerClient.Close, nil)
	kgin.KGin.Use("rocketmq", consumer.ConsumerClient.Init, consumer.ConsumerClient.Close, nil)
	time.Sleep(time.Second * 2)
	consumer.ConsumerClient.Close()
	kgin.KGin.Use("rocketmq", consumer.ConsumerClient.Init, consumer.ConsumerClient.Close, nil)
	select {}
}

func TestRocketMqByConfig2(t *testing.T) {
	mc := model.Config{
		NameServers: []string{"192.168.20.135:9876"},
		ProductConfig: model.ProductConfig{
			RetryCount:     2,
			TopicQueueNums: 16,
			Timeout:        5,
			Group:          "sjProductGroup",
		},
		ConsumerConfig: model.ConsumerConfig{
			Timeout: 5,
			Group:   "sjConsumerGroup2",
		},
	}
	consumer.ConsumerClient.InitConfig(&mc)
	go ListenRMQ()
	time.Sleep(time.Second * 2)
	consumer.ConsumerClient.Close()
	fmt.Println("9999")
	time.Sleep(time.Second * 2)
	fmt.Println("99999")
	consumer.ConsumerClient.InitConfig(&mc)
	go ListenRMQ()
	select {}
}
