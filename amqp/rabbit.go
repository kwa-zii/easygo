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
	Conf     amqpConf
	amqpConn *amqp.Connection
	amqpCh   *amqp.Channel
	amqpURL  string
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
	r.amqpURL = r.buildAmqpURL()
	r.amqpConn, err = amqp.Dial(r.amqpURL)
	beego.Debug(r.amqpURL)

	if err != nil {
		defer r.amqpConn.Close()
		beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
		panic(err)
	}

	r.amqpCh, err = r.amqpConn.Channel()
	if err != nil {
		defer r.amqpCh.Close()
		beego.Error("Failed to open channel", err)
		panic(err)
	}
}

// SendMQ 发送消息
func (r *Rabbit) SendMQ(queueName string, v interface{}) {
	var err error
	var msgContent []byte
	msgContent, err = json.Marshal(v)
	if err != nil {
		beego.Error("Marshal msg err ! ", err)
	}

	exchangeName := ""
	if r.amqpCh == nil {
		r.InitRabbitMQ()
	}

	var q amqp.Queue
	q, err = r.amqpCh.QueueDeclare(
		queueName, //name
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
		exchangeName,
		q.Name,
		false,
		false,
		amqp.Publishing{
			Headers:     amqp.Table{},
			ContentType: "text/plain",
			Body:        []byte(msgContent),
		})

}

// ConsumeQueue 消费队列 简单模式
func (r *Rabbit) ConsumeQueue(queueName string, autoAck bool) (<-chan amqp.Delivery, error) {
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	// failOnError(err, "Failed to connect to RabbitMQ")
	// defer conn.Close()

	// ch, err := conn.Channel()
	// failOnError(err, "Failed to open a channel")
	// defer ch.Close()
	r.InitRabbitMQ()
	q, err := r.amqpCh.QueueDeclare(
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

	msgs, err := r.amqpCh.Consume(
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

	// for d := range msgs {
	// 	log.Printf("Received a message: %s", d.Body)
	// 	time.Sleep(0)
	// }
}

// ConsumeFanout 消费队列 广播模式
func (r *Rabbit) ConsumeFanout(exchName string, nodeName string, autoAck bool) (<-chan amqp.Delivery, error) {
	exchangeName := "tss_cmd"
	if exchName != "" {
		exchangeName = exchName
	}
	exchType := "fanout"

	if r.amqpCh == nil {
		r.InitRabbitMQ()
	}

	err := r.amqpCh.ExchangeDeclare(
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

	q, err := r.amqpCh.QueueDeclare(
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

	err = r.amqpCh.QueueBind(
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
	msgs, err := r.amqpCh.Consume(
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
	if exchType == "" {
		exchType = "direct"
	}

	if r.amqpCh == nil {
		r.InitRabbitMQ()
	}
	exchangeName := "tss_task"
	if exchName != "" {
		exchangeName = exchName
	}

	err := r.amqpCh.ExchangeDeclare(
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
	q, err := r.amqpCh.QueueDeclare(
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
		err = r.amqpCh.QueueBind(
			q.Name,       // queue name
			routekey,     // routing key
			exchangeName, // exchange
			false,
			nil)
		if err != nil {
			beego.Error("Failed to bind a queue. ", err)
		}
	}

	msgs, err := r.amqpCh.Consume(
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

	// go func() {
	// 	for d := range msgs {
	// 		log.Printf(" [x] %s", d.Body)
	// 	}
	// }()

	// log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	// <-forever
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
