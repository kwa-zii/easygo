package amqp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

type amqpConf struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Vhost  string `json:"vhost"`
}

// Rabbit entity
type Rabbit struct {
	Conf        amqpConf
	amqpConn    *amqp.Connection
	amqpCh      *amqp.Channel
	connConsume *amqp.Connection
	//chConsume   *amqp.Channel
	amqpURL string
}

// NewRabbit RabbitMQ构造方法
func NewRabbit(conf interface{}) *Rabbit {
	var res = new(Rabbit)
	confStr, err := json.Marshal(conf)
	if err != nil {
		beego.Error(err)
	}
	err = json.Unmarshal(confStr, &res.Conf)
	if err != nil {
		beego.Error(err)
	}
	return res
}

// ShowAmqpURL RabbitMQ构造方法
func (r *Rabbit) ShowAmqpURL() {
	//fmt.Printf("Config: %v\n", r.Conf)
	// beeLogger.Log.Infof("Config: %v\n", r.Conf)
	beego.Emergency("Config: ", r.Conf)
	beego.Alert("Config: ", r.Conf)
	beego.Critical("Config: ", r.Conf)
	beego.Error("Config: ", r.Conf)
	beego.Warning("Config: ", r.Conf)
	beego.Warn("Config: ", r.Conf)
	beego.Notice("Config: ", r.Conf)
	beego.Info("Config: ", r.Conf)
	beego.Debug("Config: ", r.Conf)
}

// BuildAmqpURL 构建amqpUrl
func (r *Rabbit) buildAmqpURL() string {
	var str string
	b := bytes.Buffer{}
	b.WriteString("amqp://")
	b.WriteString(r.Conf.User)
	b.WriteString(":")
	b.WriteString(r.Conf.Passwd)
	b.WriteString("@")
	b.WriteString(r.Conf.Host)
	b.WriteString(":")
	b.WriteString(r.Conf.Port)
	b.WriteString("/")
	if r.Conf.Vhost != "/" {
		b.WriteString(r.Conf.Vhost)
	}
	str = b.String()
	return str
}

// InitRabbitMQ 初始化RabbitMQ连接和频道
func (r *Rabbit) InitRabbitMQ() {
	var err error
	// if r.amqpConn == nil {
	// 	r.amqpURL = r.buildAmqpURL()
	// 	r.amqpConn, err = amqp.Dial(r.amqpURL)
	// 	beego.Debug(r.amqpURL)

	// 	if err != nil {
	// 		defer r.amqpConn.Close()
	// 		beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
	// 		panic(err)
	// 	}
	// }

	if r.connConsume == nil {
		r.amqpURL = r.buildAmqpURL()
		r.connConsume, err = amqp.Dial(r.amqpURL)
		beego.Debug(r.amqpURL)

		if err != nil {
			defer r.connConsume.Close()
			beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
			panic(err)
		}
	}
}

//SendMQ 发送消息
func (r *Rabbit) SendMQ(queueName string, v interface{}) {
	var err error
	var msgContent []byte
	msgContent, err = json.Marshal(v)
	if err != nil {
		beego.Error("Marshal msg err ! ", err)
	}

	if r.amqpConn == nil {
		r.amqpURL = r.buildAmqpURL()
		r.amqpConn, err = amqp.Dial(r.amqpURL)
		beego.Debug(r.amqpURL)

		if err != nil {
			defer r.amqpConn.Close()
			beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
			panic(err)
		}
	}

	if r.amqpCh == nil {
		r.amqpCh, err = r.amqpConn.Channel()
		if err != nil {
			defer r.amqpCh.Close()
			beego.Error("Failed to open channel", err)
			panic(err)
		}
	}

	exchangeName := ""
	var q amqp.Queue
	q, err = r.amqpCh.QueueDeclare(
		queueName, //Queue name
		true,      //durable
		false,     //delete when unused
		false,     //exclusive
		false,     //no-wait
		nil,       //arguments
	)
	if strings.Contains(fmt.Sprintf("%s", err), "channel/connection is not open") {
		r.InitRabbitMQ()
	}

	r.amqpCh.Publish(
		exchangeName, //exchange Name
		q.Name,       //key
		false,        //mandatory
		false,        //immediate
		amqp.Publishing{
			Headers:     amqp.Table{},
			ContentType: "text/plain",
			Body:        []byte(msgContent),
		})

}

// ConsumeQueue 消费队列 简单模式
func (r *Rabbit) ConsumeQueue(queueName string, autoAck bool) (<-chan amqp.Delivery, error) {
	var err error
	var ch *amqp.Channel

	r.InitRabbitMQ()
	ch, err = r.connConsume.Channel()
	if err != nil {
		defer ch.Close()
		beego.Error("Failed to open channel", err)
		panic(err)
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		beego.Error("Failed to declare a queue ! ", err)
	}

	msgs, err := ch.Consume(
		q.Name,  // queue
		"",      // consumer
		autoAck, // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		beego.Error("Failed to register a consumer! ", err)
	}

	return msgs, err
}

// ConsumeFanout 消费队列 广播模式
func (r *Rabbit) ConsumeFanout(exchName string, nodeName string, autoAck bool) (<-chan amqp.Delivery, error) {
	var err error
	var ch *amqp.Channel

	r.InitRabbitMQ()
	ch, err = r.connConsume.Channel()
	if err != nil {
		defer ch.Close()
		beego.Error("Failed to open channel", err)
		panic(err)
	}

	exchangeName := "tss_cmd"
	if exchName != "" {
		exchangeName = exchName
	}
	exchType := "fanout"

	err = ch.ExchangeDeclare(
		exchangeName, // name
		exchType,     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		beego.Error("Failed to declare an exchange", err)
		panic(err)
	}

	q, err := ch.QueueDeclare(
		nodeName, // name
		false,    // durable
		false,    // delete when unused
		true,     // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		beego.Error("Failed to declare a queue. ", err)
	}

	err = ch.QueueBind(
		q.Name,       // queue name
		nodeName,     // routing key
		exchangeName, // exchange
		false,
		nil)
	if err != nil {
		beego.Error("Failed to bind a queue. ", err)
	}

	// var msgs <-chan amqp.Delivery
	// var errs error
	msgs, err := ch.Consume(
		q.Name,  // queue
		"",      // consumer
		autoAck, // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)

	if err != nil {
		beego.Error("Failed to register a consumer. ", err)
	}
	// for d := range msgs {
	// 	log.Printf(" [x] %s", d.Body)
	// }
	return msgs, err
}

// ConsumeRouteKey 消费队列 路由键模式
func (r *Rabbit) ConsumeRouteKey(exchType string, exchName string, queueNamePrefix string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, error) {

	var err error
	var ch *amqp.Channel

	r.InitRabbitMQ()

	ch, err = r.connConsume.Channel()
	if err != nil {
		defer ch.Close()
		beego.Error("Failed to open channel", err)
		panic(err)
	}

	if exchType == "" {
		exchType = "direct"
	}

	exchangeName := "tss_task"
	if exchName != "" {
		exchangeName = exchName
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		exchType,     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		beego.Error("Failed to declare an exchange. ", err)
	}

	queueName := queueNamePrefix + strings.Join(routeKeys, "_")
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		beego.Error("Failed to declare a queue. ", err)
	}

	for _, routekey := range routeKeys {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, exchangeName, routekey)
		err = ch.QueueBind(
			q.Name,       // queue name
			routekey,     // routing key
			exchangeName, // exchange
			false,        // no-wait
			nil)
		if err != nil {
			beego.Error("Failed to bind a queue. ", err)
		}
	}

	msgs, err := ch.Consume(
		q.Name,  // queue
		"",      // consumer
		autoAck, // auto ack
		false,   // exclusive
		false,   // no local
		false,   // no wait
		nil,     // args
	)
	if err != nil {
		beego.Error("Failed to register a consumer. ", err)
	}

	return msgs, err
}

// ConsumeDirect 消费队列
func (r *Rabbit) ConsumeDirect(exchName string, queueName string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, error) {
	exchType := "direct"
	//queueName := nodeName
	return r.ConsumeRouteKey(exchType, exchName, queueName, routeKeys, autoAck)
}

// ConsumeTopic 消费队列 订阅模式
func (r *Rabbit) ConsumeTopic(exchName string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, error) {
	exchType := "topic"
	//queueNamePrefix := nodeName + "_"
	queueNamePrefix := ""
	return r.ConsumeRouteKey(exchType, exchName, queueNamePrefix, routeKeys, autoAck)
}
