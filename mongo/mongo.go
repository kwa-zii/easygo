package mongo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/astaxie/beego"
	mgo "gopkg.in/mgo.v2"
)

type mongoConf struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Db     string `json:"db"`
}

// Mgo m
type Mgo struct {
	conf     mongoConf
	session  *mgo.Session
	database *mgo.Database
}

// NewMongodb mongo connection
func NewMongodb(mgoConf string) *Mgo {
	res := new(Mgo)
	err := json.Unmarshal([]byte(mgoConf), &res.conf)
	if err != nil {
		beego.Error(err)
	}
	beego.Debug(res.conf)

	dialInfo := &mgo.DialInfo{
		Addrs:     []string{fmt.Sprintf("%s:%s", res.conf.Host, res.conf.Port)},
		Database:  res.conf.Db,
		Username:  res.conf.User,
		Password:  res.conf.Passwd,
		Direct:    false,
		Timeout:   time.Second * 3,
		PoolLimit: 4096,
		// Session.SetPoolLimit
	}
	//创建一个维护套接字池的session
	res.session, err = mgo.DialWithInfo(dialInfo)

	mongoAddr := buildMongoURL(res.conf)
	// res.session, err = mgo.Dial(mongoAddr)
	if err != nil {
		beego.Error("Connect mongo error. ", err)
		panic(err)
	}
	res.session.SetMode(mgo.Monotonic, true)
	database := res.conf.Db
	res.database = res.session.DB(database)
	//defer session.Close()
	beego.Debug("Config: ", mongoAddr)
	beego.Info("Init mongo complete")

	return res
}

func buildMongoURL(conf mongoConf) string {
	var str string
	b := bytes.Buffer{}
	if conf.User != "" {
		b.WriteString(conf.User)
		b.WriteString(":")
		b.WriteString(conf.Passwd)
		b.WriteString("@")
	}

	b.WriteString(conf.Host)
	b.WriteString(":")
	b.WriteString(conf.Port)
	b.WriteString("/")
	b.WriteString(conf.Db)
	str = b.String()
	return str
}

func (h *Mgo) reconnect() {
	if h.session == nil {
		var err error
		mongoAddr := buildMongoURL(h.conf)
		h.session, err = mgo.Dial(mongoAddr)
		if err != nil {
			beego.Error("Connect mongo error. ", err)
			panic(err)
		}
		h.session.SetMode(mgo.Monotonic, true)
		database := h.conf.Db
		h.database = h.session.DB(database)
		//defer session.Close()
		beego.Info("reconnect mongo complete")
		beego.Debug("Config: ", mongoAddr)
	}
}

// GetMgo get session
func (h *Mgo) GetMgo() *mgo.Session {
	return h.session
}

// GetDataBase get database
func (h *Mgo) GetDataBase() *mgo.Database {
	return h.database
}

// SetDataBase set DataBase
func (h *Mgo) SetDataBase(database string) *mgo.Database {
	h.conf.Db = database
	return h.session.DB(database)
}

// GetErrNotFound get error
func (h *Mgo) GetErrNotFound() error {
	return mgo.ErrNotFound
}

// QueryCount get document count
func (h *Mgo) QueryCount(collName string) int {
	collection := h.database.C(collName)
	//condition := bson.M{"status": 1}
	//count, err := collection.Find(condition).Count()
	count, err := collection.Count()
	if err != nil {
		beego.Error("query collection count error . ", err)
	}

	return count
}

// GetAll get all document list
func (h *Mgo) GetAll(l int, s int, collName string) ([]interface{}, int, error) {
	var result []interface{}
	collection := h.database.C(collName)
	total, _ := collection.Find(nil).Count()
	err := collection.Find(nil).Limit(l).Skip(s).All(&result)
	if err != nil {
		beego.Error("query collection err. ", err)
	}

	return result, total, err
}

// GetList get document list
func (h *Mgo) GetList(c map[string]interface{}, collName string) ([]interface{}, int, error) {
	var result []interface{}
	collection := h.database.C(collName)
	total, err := collection.Find(c).Count()
	err = collection.Find(c).All(&result)
	if err != nil {
		beego.Error("query collection err. ", err)
	}
	return result, total, err
}

// GetOne get document
func (h *Mgo) GetOne(c map[string]interface{}, collName string, result interface{}) error {
	collection := h.database.C(collName)
	err := collection.Find(c).One(result)
	return err
}

// Save document
func (h *Mgo) Save(selector, update interface{}, collName string) error {
	collection := h.database.C(collName)
	err := collection.Update(selector, update)
	return err
}

// Insert new document
func (h *Mgo) Insert(info interface{}, collName string) error {
	collection := h.database.C(collName)
	err := collection.Insert(info)
	if err != nil {
		beego.Error("insert collection err. ", err)
	}
	return err
}
