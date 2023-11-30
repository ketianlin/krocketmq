package consumer

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/ketianlin/kgin/logs"
	"github.com/ketianlin/krocketmq/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"io/ioutil"
	"log"
	"strings"
	"time"
)

type consumerClient struct {
	conf       *koanf.Koanf
	confUrl    string
	conn       rocketmq.PushConsumer
	closeError error
}

var ConsumerClient = &consumerClient{}
var logger = gologger.GetLogger()

func (r *consumerClient) InitConfig(conf *model.Config, callback func(im *model.InitCallbackMessage)) {
	if r.conn == nil {
		c, err := rocketmq.NewPushConsumer(
			consumer.WithNameServer(conf.NameServers), // 接入点地址
			consumer.WithConsumerModel(consumer.Clustering),
			consumer.WithGroupName(conf.ConsumerConfig.Group), // 分组名称
			consumer.WithConsumeTimeout(time.Duration(conf.ConsumerConfig.Timeout)*time.Second),
		)
		cm := new(model.InitCallbackMessage)
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ创建消费者错误:%s\n", err.Error()))
			cm.InitError = err
		} else {
			r.conn = c
			logger.Debug("当前 krocketmq 版本：v1.0.5")
			cm.Version = "当前 krocketmq 版本：v1.0.5"
			cm.IsSuccessful = true
		}
		callback(cm)
	}
}

func (r *consumerClient) Init(rocketmqConfigUrl string) {
	if rocketmqConfigUrl != "" {
		r.confUrl = rocketmqConfigUrl
	}
	if r.confUrl == "" {
		logger.Error("rocketmq配置Url为空")
		return
	}
	if r.conn == nil {
		if r.conf == nil {
			var confData []byte
			var err error
			if strings.HasPrefix(r.confUrl, "http://") {
				resp, err := grequests.Get(r.confUrl, nil)
				if err != nil {
					logs.Error("RocketMQ配置下载失败!{} ", err.Error())
					return
				}
				confData = []byte(resp.String())
			} else {
				confData, err = ioutil.ReadFile(r.confUrl)
				if err != nil {
					logs.Error("RocketMQ本地配置文件{}读取失败:{}", r.confUrl, err.Error())
					return
				}
			}
			r.conf = koanf.New(".")
			err = r.conf.Load(rawbytes.Provider(confData), yaml.Parser())
			if err != nil {
				logs.Error("RocketMQ配置解析错误:{}", err.Error())
				r.conf = nil
				return
			}
		}
		nameServers := r.conf.Strings("go.rocketmq.name_servers")
		timeout := r.conf.Int("go.rocketmq.consumer.timeout")
		consumerGroup := r.conf.String("go.rocketmq.consumer.group")
		c, err := rocketmq.NewPushConsumer(
			consumer.WithNameServer(nameServers), // 接入点地址
			consumer.WithConsumerModel(consumer.Clustering),
			consumer.WithGroupName(consumerGroup), // 分组名称
			consumer.WithConsumeTimeout(time.Duration(timeout)*time.Second),
		)
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ创建消费者错误:%s\n", err.Error()))
		} else {
			r.conn = c
		}
	}
}

func (r *consumerClient) GetCloseError() error {
	return r.closeError
}

func (r *consumerClient) Close() {
	if r.conn != nil {
		err := r.conn.Shutdown()
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ关闭消费者client错误:%s\n", err.Error()))
			r.closeError = err
			return
		}
	}
	r.conn = nil
}

func (r *consumerClient) MessageListener(topicName string, listener func(msg []byte), callbacks ...func(err error)) {
	err := r.conn.Subscribe(topicName, consumer.MessageSelector{}, func(ctx context.Context, msg ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, v := range msg {
			//fmt.Println("收到：", string(v.Body)) // v.Body : 消息主体
			go listener(v.Body)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		logger.Error(fmt.Sprintf("RocketMQ消费者监听错误:%s\n", err.Error()))
		if len(callbacks) > 0 {
			callbacks[0](err)
		}
	}
	forever := make(chan bool)
	err = r.conn.Start()
	if err != nil {
		defer func(conn rocketmq.PushConsumer) {
			err := conn.Shutdown()
			if err != nil {
				logger.Error(fmt.Sprintf("RocketMQ消费者监听错误后关闭:%s\n", err.Error()))
				if len(callbacks) > 0 {
					callbacks[0](err)
				}
			}
		}(r.conn)
		log.Fatal(err)
	}
	<-forever
}
