package config

import (
	"fmt"
	"gopkg.in/ini.v1"
)

type Config struct {
	Etcd    `ini:"etcd"`
	Kafka   `ini:"kafka"`
	Log     `ini:"log"`
	Elastic `ini:"elastic"`
}

type Etcd struct {
	Addr string		`ini:"addr"`
}

type Kafka struct {
	Addr string		`ini:"addr"`
}

type Elastic struct {
	Addr string		`ini:"addr"`
	UserName string	`ini:"username"`
	Password string	`ini:"password"`
}

type Log struct {
	Topic string		`ini:"topic"`
	EtcdKey string		`ini:"etcd_key"`
}

func NewConfig(configPath string) *Config {
	Config := &Config{}
	err := ini.MapTo(Config, configPath)
	if err != nil {
		fmt.Printf("get location configs from %v fail, %v \n", configPath, err)
	}

	return Config
}