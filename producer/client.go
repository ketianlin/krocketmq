package producer

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/ketianlin/kgin/logs"
	"github.com/ketianlin/krocketmq/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"io/ioutil"
	"strings"
	"time"
)

type producerClient struct {
	conf       *koanf.Koanf
	confUrl    string
	conn       rocketmq.Producer
	closeError error
}

var ProducerClient = &producerClient{}
var logger = gologger.GetLogger()

func (r *producerClient) Init(rocketmqConfigUrl string) {
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
		retryCount := r.conf.Int("go.rocketmq.producer.retry_count")
		timeout := r.conf.Int("go.rocketmq.producer.timeout")
		topicQueueNums := r.conf.Int("go.rocketmq.producer.topic_queue_nums")
		productGroup := r.conf.String("go.rocketmq.producer.group")
		p, err := rocketmq.NewProducer(
			producer.WithNameServer(nameServers), // 接入点地址
			producer.WithRetry(retryCount),       // 重试次数
			producer.WithGroupName(productGroup), // 分组名称
			producer.WithSendMsgTimeout(time.Duration(timeout)*time.Second),
			producer.WithDefaultTopicQueueNums(topicQueueNums),
		)
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ创建生产者错误:%s\n", err.Error()))
		} else {
			r.conn = p
		}
	}
}

func (r *producerClient) InitConfig(conf *model.Config, callback func(err error)) {
	if r.conn == nil {
		p, err := rocketmq.NewProducer(
			producer.WithNameServer(conf.NameServers),         // 接入点地址
			producer.WithRetry(conf.ProductConfig.RetryCount), // 重试次数
			producer.WithGroupName(conf.ProductConfig.Group),  // 分组名称
			producer.WithSendMsgTimeout(time.Duration(conf.ProductConfig.Timeout)*time.Second),
			producer.WithDefaultTopicQueueNums(conf.ProductConfig.TopicQueueNums),
		)
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ创建生产者错误:%s\n", err.Error()))
			callback(err)
		} else {
			r.conn = p
		}
	}
}

func (r *producerClient) GetConnection() rocketmq.Producer {
	return r.conn
}

func (r *producerClient) Close() {
	if r.conn != nil {
		err := r.conn.Shutdown()
		if err != nil {
			logger.Error(fmt.Sprintf("RocketMQ关闭生产者client错误:%s\n", err.Error()))
			r.closeError = err
			return
		}
	}
	r.conn = nil
}

// GetCloseError 获取关闭的error
func (r *producerClient) GetCloseError() error {
	return r.closeError
}

func (r *producerClient) SendSync(message *model.TopicMessage) error {
	var err error
	err = r.conn.Start()
	if err != nil {
		logger.Error(fmt.Sprintf("RocketMQ生产者Start错误:%s\n", err.Error()))
		return err
	}
	msg := &primitive.Message{
		Topic: message.TopicName,
		Body:  []byte(message.Msg),
	}
	msg.WithTag(message.Tags)
	msg.WithKeys(message.Keys)

	_, err = r.conn.SendSync(context.Background(), msg)
	//fmt.Println(a)
	return err
}

func (r *producerClient) SendOne(message *model.TopicMessage) error {
	var err error
	err = r.conn.Start()
	if err != nil {
		logger.Error(fmt.Sprintf("RocketMQ生产者Start错误:%s\n", err.Error()))
		return err
	}
	msg := &primitive.Message{
		Topic: message.TopicName,
		Body:  []byte(message.Msg),
	}
	msg.WithTag(message.Tags)
	msg.WithKeys(message.Keys)
	err = r.conn.SendOneWay(context.Background(), msg)
	return err
}
