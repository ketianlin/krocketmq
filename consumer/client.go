package consumer

import (
	"context"
	"errors"
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
	"runtime"
	"strings"
	"time"
)

type consumerClient struct {
	conf       *koanf.Koanf
	confUrl    string
	conn       rocketmq.PushConsumer
	closeError error
	config     *model.Config
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
			logger.Debug("当前 krocketmq 版本：v1.0.9")
			cm.Version = "当前 krocketmq 版本：v1.0.9"
			cm.IsSuccessful = true
			r.config = conf
		}
		// 设置定时任务自动检查
		//ticker := time.NewTicker(time.Minute * time.Duration(conf.ConsumerConfig.MonitoringTime))
		ticker := time.NewTicker(time.Second * time.Duration(conf.ConsumerConfig.MonitoringTime))
		go func() {
			for _ = range ticker.C {
				_ = r.MqCheck()
			}
		}()
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
		defer func() {
			if e := recover(); e != nil {
				switch e := e.(type) {
				case string:
					r.closeError = errors.New(e)
				case runtime.Error:
					r.closeError = errors.New(e.Error())
				case error:
					r.closeError = e
				default:
					r.closeError = errors.New("RocketMQ关闭消费者client错误")
				}
			}
		}()
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
		logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题失败，错误:%s\n", topicName, err.Error()))
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
				logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题错误后关闭:%s\n", topicName, err.Error()))
				if len(callbacks) > 0 {
					callbacks[0](err)
				}
			}
		}(r.conn)
		log.Fatal(err)
	}
	<-forever
}

func (r *consumerClient) MessageListenerNew(topicName string, listener func(topicName string, msg []byte), callbacks ...func(err error)) {
	err := r.conn.Subscribe(topicName, consumer.MessageSelector{}, func(ctx context.Context, msg ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, v := range msg {
			//fmt.Println("收到：", string(v.Body)) // v.Body : 消息主体
			go listener(topicName, v.Body)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题失败，错误:%s\n", topicName, err.Error()))
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
				logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题错误后关闭:%s\n", topicName, err.Error()))
				if len(callbacks) > 0 {
					callbacks[0](err)
				}
			}
		}(r.conn)
		log.Fatal(err)
	}
	<-forever
}

func (r *consumerClient) MqCheck() error {
	if r.conn == nil {
		fmt.Println("MQ连接 r.conn 连接为空：准备初始化MQ连接")
		if r.confUrl != "" {
			logs.Debug("{} 连接不为空，使用配置文件重新初始化MQ连接", r.confUrl)
			r.Init(r.confUrl)
		} else {
			logs.Debug("{} 配置类不为空，使用配置类重新初始化MQ连接", r.config)
			r.InitConfig(r.config, func(im *model.InitCallbackMessage) {
				logs.Debug("MqCheck running...")
				logs.Debug("InitCallbackMessage: {}", im)
			})
		}
	}
	return nil
}
