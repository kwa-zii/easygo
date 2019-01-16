package amqp

import (
	"bytes"
	"encoding/json"

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

// Broker entity
type Broker struct {
	Conf         amqpConf
	connProducer *amqp.Connection
	chProducer   *amqp.Channel
	connConsumer *amqp.Connection
	//chConsumer  *amqp.Channel
	amqpURL string
}

// NewBroker 构造方法
func NewBroker(conf interface{}) *Broker {
	var res = new(Broker)
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

// ShowAmqpURL 输出Amqp连接信息
func (r *Broker) ShowAmqpURL() {
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
func (r *Broker) buildAmqpURL() string {
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
