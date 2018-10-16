package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/astaxie/beego"
	"gopkg.in/yaml.v2"
)

//var confDir string = "/usr/local/etc/" + AppName

//Configure config
type Configure struct {
	Conf    interface{}
	appName string
	confDir string
}

//DirStruct the directory struct
type DirStruct struct {
	TargetBinDir    string `json:"target_bin_dir"`
	TargetConfigDir string `json:"target_config_dir"`
	TargetLogDir    string `json:"target_log_dir"`
	TargetDataDir   string `json:"target_data_dir"`
	TargetTempDir   string `json:"target_temp_dir"`
}

//CommonConf common config
type CommonConf struct {
	NodeName        string `json:"node_name"`
	MasterHost      string `json:"master_host"`
	AppKey          string `json:"app_key"`
	SecKey          string `json:"sec_key"`
	MaxTaskConsumer int    `json:"max_task_consumer"`
}

//RabbitMQConf rabbitmq config
type RabbitMQConf struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Vhost  string `json:"vhost"`
}

//RedisConf redis config
type RedisConf struct {
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Db       string `json:"db"`
	Passwd   string `json:"passwd"`
}

//MongoConf mongo config
type MongoConf struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Db     string `json:"db"`
}

// NewConfigure Construct Func
func NewConfigure(appName string, conf interface{}) *Configure {
	res := new(Configure)
	confDir := "/usr/local/etc/" + appName
	res.confDir = confDir
	res.appName = appName
	res.Conf = conf
	return res
}

//InitConfigFile 初始化配置文件
func (h *Configure) InitConfigFile() {

	currentPath, err := os.Getwd()
	if err != nil {
		beego.Error(err.Error())
	}
	bytes, _ := json.Marshal(h.Conf)
	fmt.Printf("Configure:\n%s\n", bytes)

	//configFile := currentPath + "/tss-monitor.dev.json"
	configFile := fmt.Sprintf("%s/%s.dev.json", currentPath, h.appName)
	err = ioutil.WriteFile(configFile, bytes, 0666)
	if err != nil {
		beego.Error("Failed to Write JSON file: ", err)
	}

	bytes, _ = yaml.Marshal(h.Conf)
	fmt.Printf("Configure:\n%s\n", bytes)
	//configFile = currentPath + "/tss-monitor.dev.yaml"
	configFile = fmt.Sprintf("%s/%s.dev.yaml", currentPath, h.appName)
	err = ioutil.WriteFile(configFile, bytes, 0666) //写入文件(字节数组)
	if err != nil {
		beego.Error("Failed to Write YAML file: ", err)
	}
}

// LoadConfig loads the bee tool configuration.
// It looks for Beefile or bee.json in the current path,
// and falls back to default configuration in case not found.
func (h *Configure) LoadConfig(env string, deployMode string, result interface{}) error {
	var err error
	var currentPath string
	if env == "prod" {
		env = ""
	} else {
		env = "." + env
	}

	if deployMode == "alone" {
		currentPath, err = os.Getwd()
		h.confDir = currentPath
	}
	if err != nil {
		beego.Error(err)
	}

	var dir *os.File
	dir, err = os.Open(h.confDir)
	if err != nil {
		beego.Error(err)
	}
	defer dir.Close()

	var files []os.FileInfo
	files, err = dir.Readdir(-1)
	if err != nil {
		beego.Error(err)
	}

	foundConf := false
	for _, file := range files {
		switch file.Name() {
		case h.appName + env + ".json":
			{
				err = parseJSON(filepath.Join(h.confDir, file.Name()), result)
				if err != nil {
					beego.Error("Failed to parse JSON file: ", err)
				}
				foundConf = true
				beego.Info("Load Configure : " + filepath.Join(h.confDir, file.Name()))
				break
			}
		case h.appName + env + ".yaml":
			{
				err = parseYAML(filepath.Join(h.confDir, file.Name()), result)
				if err != nil {
					beego.Error("Failed to parse YAML file: ", err)
				}
				foundConf = true
				beego.Info("Load Configure : " + filepath.Join(h.confDir, file.Name()))
				break
			}
		}
		if foundConf == true {
			h.Conf = result
			break
		} else {
			err = errors.New("Configure file is not found in " + h.confDir)
		}
	}
	return err
}

func parseJSON(path string, v interface{}) error {
	var (
		data []byte
		err  error
	)
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, v)
	return err
}

func parseYAML(path string, v interface{}) error {
	var (
		data []byte
		err  error
	)
	data, err = ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, v)
	return err
}
