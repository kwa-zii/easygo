package amqp

import (
	"encoding/json"
	"time"

	"github.com/astaxie/beego"
	"github.com/streadway/amqp"
)

// InitProducer 初始化生产连接和频道
func (r *Broker) InitProducer() {
	var err error

	if r.connProducer == nil {
		r.amqpURL = r.buildAmqpURL()
		r.connProducer, err = amqp.Dial(r.amqpURL)
		beego.Debug(r.amqpURL)
		if err != nil {
			defer r.connProducer.Close()
			beego.Error("Failed to connect tp rabbitmq", err, r.amqpURL)
			panic(err)
		}
	}
	beego.Debug(r.connProducer.LocalAddr())

	if r.chProducer == nil {
		r.chProducer, err = r.connProducer.Channel()
		if err != nil {
			beego.Error(err)
			defer r.chProducer.Close()
			beego.Error("Failed to open channel", err)
			panic(err)
		}

		//channel意外关闭时，重新建立连接和channel
		go func() {
			cc := make(chan *amqp.Error)
			e := <-r.chProducer.NotifyClose(cc)
			beego.Error("[RABBITMQ_CLIENT]", "channel close error:", e.Error())
			r.chProducer = nil
			r.connProducer = nil
			r.InitProducer()
			time.Sleep(1)
		}()
	}
}

//SendMQ 发送消息
func (r *Broker) SendMQ(queueName string, v interface{}) error {
	var err error
	var msgContent []byte
	msgContent, err = json.Marshal(v)
	if err != nil {
		beego.Error("Marshal msg err ! ", err)
	}

	r.InitProducer()

	exchangeName := ""
	var q amqp.Queue
	q, err = r.chProducer.QueueDeclare(
		queueName, //Queue name
		true,      //durable
		false,     //delete when unused
		false,     //exclusive
		false,     //no-wait
		nil,       //arguments
	)
	// if strings.Contains(fmt.Sprintf("%s", err), "channel/connection is not open") {
	// 	r.InitProducer()
	// }

	err = r.chProducer.Publish(
		exchangeName, //exchange Name
		q.Name,       //key
		false,        //mandatory
		false,        //immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      amqp.Table{},
			ContentType:  "text/plain",
			Body:         []byte(msgContent),
		})
	return err

}
