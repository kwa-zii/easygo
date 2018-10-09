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
	count, err := collection.Count()
	if err != nil {
		beego.Error("query collection count error . ", err)
	}
	return count
}

// GetAll get all document list
func (h *Mgo) GetAll(collName string, limit, skip int, result interface{}, total *int) error {
	collection := h.database.C(collName)
	*total, _ = collection.Find(nil).Count()
	err := collection.Find(nil).Limit(limit).Skip(skip).All(result)
	return err
}

// GetList get document list
func (h *Mgo) GetList(collName string, c map[string]interface{}, limit, skip int, result interface{}, total *int) error {
	collection := h.database.C(collName)
	*total, _ = collection.Find(c).Count()
	err := collection.Find(c).Limit(limit).Skip(skip).All(result)
	return err
}

// GetOne get document
func (h *Mgo) GetOne(query interface{}, collName string, result interface{}) error {
	collection := h.database.C(collName)
	err := collection.Find(query).One(result)
	return err
}

// Update document
func (h *Mgo) Update(collName string, selector, update interface{}) error {
	collection := h.database.C(collName)
	err := collection.Update(selector, update)
	return err
}

// Insert new document
func (h *Mgo) Insert(collName string, docs interface{}) error {
	collection := h.database.C(collName)
	err := collection.Insert(docs)
	return err
}
