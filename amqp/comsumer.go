package amqp

import (
	"log"
	"strings"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

// InitConsumer 初始化消费连接和频道
func (r *Broker) InitConsumer() {
	var err error
	if r.connConsumer == nil {
		r.amqpURL = r.buildAmqpURL()
		r.connConsumer, err = amqp.Dial(r.amqpURL)
		beego.Debug(r.amqpURL)

		if err != nil {
			defer r.connConsumer.Close()
			beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
			panic(err)
		}
	}
}

// ConsumeQueue 消费队列 简单模式
func (r *Broker) ConsumeQueue(queueName string, autoAck bool) (<-chan amqp.Delivery, error) {
	var err error
	var ch *amqp.Channel

	r.InitConsumer()
	ch, err = r.connConsumer.Channel()
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
func (r *Broker) ConsumeFanout(exchName string, nodeName string, autoAck bool) (<-chan amqp.Delivery, error) {
	var err error
	var ch *amqp.Channel

	r.InitConsumer()
	ch, err = r.connConsumer.Channel()
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
func (r *Broker) ConsumeRouteKey(exchType string, exchName string, queueNamePrefix string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, *amqp.Channel, error) {

	var err error
	var ch *amqp.Channel

	r.InitConsumer()

	ch, err = r.connConsumer.Channel()
	if err != nil {
		defer ch.Close()
		beego.Error("Failed to open channel", err)
		panic(err)
	}
	ch.Qos(1, 0, false)

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

	return msgs, ch, err
}

// ConsumeDirect 消费队列
func (r *Broker) ConsumeDirect(exchName string, queueName string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, *amqp.Channel, error) {
	exchType := "direct"
	//queueName := nodeName
	return r.ConsumeRouteKey(exchType, exchName, queueName, routeKeys, autoAck)
}

// ConsumeTopic 消费队列 订阅模式
func (r *Broker) ConsumeTopic(exchName string, routeKeys []string, autoAck bool) (<-chan amqp.Delivery, *amqp.Channel, error) {
	exchType := "topic"
	//queueNamePrefix := nodeName + "_"
	queueNamePrefix := ""
	return r.ConsumeRouteKey(exchType, exchName, queueNamePrefix, routeKeys, autoAck)
}
