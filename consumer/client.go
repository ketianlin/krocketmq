package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/ketianlin/kgin/logs"
	"github.com/ketianlin/krocketmq/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"io/ioutil"
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
	//timeTicker            *time.Ticker
	stopMqCheckTickerChan chan bool
}

var ConsumerClient = &consumerClient{}
var logger = gologger.GetLogger()

func (r *consumerClient) InitConfig(conf *model.Config, callback func(im *model.InitCallbackMessage)) {
	if r.conn == nil {
		// 日志级别设置
		logLevel := r.getLogLevel(conf.ConsumerConfig.LogLevel)
		rlog.SetLogLevel(logLevel)
		c, err := rocketmq.NewPushConsumer(
			consumer.WithNameServer(conf.NameServers),         // 接入点地址
			consumer.WithConsumerModel(consumer.Clustering),   // 一条消息在一个组中只有一个consumer消费
			consumer.WithGroupName(conf.ConsumerConfig.Group), // 分组名称
			consumer.WithConsumerOrder(true),                  // *顺序接收 必须
			//consumer.WithConsumeTimeout(time.Duration(conf.ConsumerConfig.Timeout)*time.Second),
		)
		cm := new(model.InitCallbackMessage)
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ创建消费者错误:%s\n", err.Error()))
			cm.InitError = err
		} else {
			r.conn = c
			logger.Debug("当前 krocketmq 版本：v1.0.18")
			cm.Version = "当前 krocketmq 版本：v1.0.18"
			r.config = conf
		}
		// 设置定时任务自动检查
		r.mqCheckTicker(conf.ConsumerConfig.MonitoringTime, callback)
		//r.timeTicker = time.NewTicker(time.Second * time.Duration(conf.ConsumerConfig.MonitoringTime))
		//go func() {
		//	for _ = range r.timeTicker.C {
		//		err2 := r.MqCheck()
		//		if err2 != nil {
		//			cm := new(model.InitCallbackMessage)
		//			cm.MqCheckError = err2
		//			callback(cm)
		//		}
		//	}
		//}()
		callback(cm)
	}
}

func (r *consumerClient) mqCheckTicker(monitoringTime int, callback func(im *model.InitCallbackMessage)) {
	// 设置定时任务自动检查
	tTicker := time.NewTicker(time.Second * time.Duration(monitoringTime))
	r.stopMqCheckTickerChan = make(chan bool)
	go func(ticker *time.Ticker, callback func(im *model.InitCallbackMessage)) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err2 := r.MqCheck()
				if err2 != nil {
					cm := new(model.InitCallbackMessage)
					cm.MqCheckError = err2
					callback(cm)
				}
			case stop := <-r.stopMqCheckTickerChan:
				if stop {
					logger.Error("关闭定时器检查rocketmq的连接是否存在任务")
					return
				}
			}
		}
	}(tTicker, callback)
}

func (r *consumerClient) getLogLevel(level string) string {
	var levelFlag string
	switch strings.ToLower(level) {
	case "debug":
		levelFlag = "debug"
	case "warn":
		levelFlag = "warn"
	case "error":
		levelFlag = "error"
	case "fatal":
		levelFlag = "fatal"
	default:
		levelFlag = "info"
	}
	return levelFlag
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
		//timeout := r.conf.Int("go.rocketmq.consumer.timeout")
		consumerGroup := r.conf.String("go.rocketmq.consumer.group")
		// 日志级别设置
		logLevel := r.getLogLevel(r.conf.String("go.rocketmq.consumer.log_level"))
		rlog.SetLogLevel(logLevel)
		c, err := rocketmq.NewPushConsumer(
			consumer.WithNameServer(nameServers),            // 接入点地址
			consumer.WithConsumerModel(consumer.Clustering), // 一条消息在一个组中只有一个consumer消费
			consumer.WithGroupName(consumerGroup),           // 分组名称
			consumer.WithConsumerOrder(true),                // *顺序接收 必须
			//consumer.WithConsumeTimeout(time.Duration(timeout)*time.Second),
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
	defer func() {
		if r.stopMqCheckTickerChan != nil {
			r.stopMqCheckTickerChan <- true
			close(r.stopMqCheckTickerChan)
		}
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
		logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题失败，错误:%s\n", topicName, err.Error()))
		if len(callbacks) > 0 {
			callbacks[0](err)
		}
	}
	forever := make(chan bool)
	err = r.conn.Start()
	if err != nil {
		//defer func(conn rocketmq.PushConsumer) {
		//
		//}(r.conn)
		err := r.conn.Shutdown()
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题错误后关闭:%s\n", topicName, err.Error()))
			if len(callbacks) > 0 {
				callbacks[0](err)
			}
		}
		//log.Fatal(err)
	}
	<-forever
}

func (r *consumerClient) MessageListenerReturnFullMessage(topicName string, listener func(msg *primitive.MessageExt), callbacks ...func(err error)) {
	defer func() {
		if r.stopMqCheckTickerChan != nil {
			r.stopMqCheckTickerChan <- true
			close(r.stopMqCheckTickerChan)
		}
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
			if len(callbacks) > 0 {
				callbacks[0](r.closeError)
			}
		}
	}()
	err := r.conn.Subscribe(topicName, consumer.MessageSelector{}, func(ctx context.Context, msg ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, v := range msg {
			go listener(v)
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
		errAll := err
		err2 := r.conn.Shutdown()
		if err2 != nil {
			errAll = errors.Join(errAll, err2)
		}
		logger.Error(fmt.Sprintf("RocketMQ消费者订阅【%s】主题错误后关闭:%s\n", topicName, errAll.Error()))
		if len(callbacks) > 0 {
			callbacks[0](err)
		}
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
		err2 := r.conn.Shutdown()
		errAll := err
		if err2 != nil {
			errAll = errors.Join(errAll, err2)
		}
		errMsg := fmt.Sprintf("RocketMQ消费者启动监听【%s】主题失败:%s\n", topicName, errAll.Error())
		logger.Error(errMsg)
		if len(callbacks) > 0 {
			callbacks[0](err)
		}
		//log.Fatal(errors.New(errMsg))
	}
	<-forever
	close(forever)
}

func (r *consumerClient) MqCheck() (err error) {
	defer func() {
		if e := recover(); e != nil {
			var errMsg string
			switch e := e.(type) {
			case string:
				errMsg = e
			case runtime.Error:
				errMsg = e.Error()
			case error:
				errMsg = e.Error()
			default:
				errMsg = "未知错误"
			}
			err = errors.New(errMsg)
		}
	}()
	var errorMsg string
	if r.conn == nil {
		errorMsg = "MQ连接 r.conn 连接为空：准备初始化MQ连接"
		fmt.Println("MQ连接 r.conn 连接为空：准备初始化MQ连接")
		if r.confUrl != "" {
			errorMsg = fmt.Sprintf("%s 连接不为空，使用配置文件重新初始化MQ连接", r.confUrl)
			logs.Debug("{} 连接不为空，使用配置文件重新初始化MQ连接", r.confUrl)
			r.Init(r.confUrl)
		} else {
			errorMsg = fmt.Sprintf("%v 配置类不为空，使用配置类重新初始化MQ连接", r.config)
			logs.Debug("{} 配置类不为空，使用配置类重新初始化MQ连接", r.config)
			r.InitConfig(r.config, func(im *model.InitCallbackMessage) {
				logs.Debug("MqCheck running...")
				logs.Debug("InitCallbackMessage: {}", im)
			})
		}
	}
	if errorMsg != "" {
		err = errors.New(errorMsg)
	}
	return err
}
