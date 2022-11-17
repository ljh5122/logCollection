package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"logCollection/pkg/config"
	"logCollection/pkg/etcd"
	"logCollection/pkg/kafka"
	"logCollection/pkg/taillog"

)

type LogAgent struct {
	kafkaProducer 	*kafka.Producer
	etcdClient 		*etcd.Client
	etcdConfigKey	string
	taskList 		map[string]*task
}

type task struct {
	tailLogObj 		*taillog.TailLog
	ctx 			context.Context
	cancelFunc		context.CancelFunc
	config			*taskConfig
}

type taskConfig struct {
	FilePath 	string	`json:"file_path"`
	Topic		string	`json:"topic"`
}

func NewLogAgent(appConfig *config.Config) *LogAgent {
	return &LogAgent{
		kafkaProducer : kafka.NewProducer(appConfig.Kafka.Addr),
		etcdClient:     etcd.NewClient(appConfig.Etcd.Addr),
		etcdConfigKey:  appConfig.Log.EtcdKey,
		taskList :      make(map[string]*task, 8),
	}
}

func (receiver *LogAgent) Run()  {
	resp := receiver.etcdClient.Get(receiver.etcdConfigKey)
	logConfigList := make([]*taskConfig, 8)
	for _, kv := range resp.Kvs {
		if err := json.Unmarshal(kv.Value, &logConfigList); err != nil {
			fmt.Println("logAgent GetConfig fail, ", err)
		}
	}

	receiver.tidyTask(logConfigList)
	go receiver.watchConfigFromEtcd()
}

func (receiver *LogAgent) watchConfigFromEtcd()  {
	watchCh := receiver.etcdClient.Watch(receiver.etcdConfigKey)
	taskConfigList := make([]*taskConfig, 8)
	for WatchResponse := range watchCh {
		for _, event := range WatchResponse.Events {
			if event.Type == etcd.EventTypePut {
				if err := json.Unmarshal(event.Kv.Value, &taskConfigList); err != nil {
					fmt.Println("configs WatchConfigFromEtcd fail, ", err)
				}
			}

			receiver.tidyTask(taskConfigList)
		}
	}
}

func (receiver *LogAgent) tidyTask(taskConfigList []*taskConfig) {
	taskConfigListMap := make(map[string]*taskConfig, 8)
	for _, taskConfig := range taskConfigList {
		if taskConfig.FilePath != "" && taskConfig.Topic != ""{
			taskKey := receiver.getTaskKey(taskConfig.FilePath, taskConfig.Topic)
			taskConfigListMap[taskKey] = taskConfig
			if receiver.taskList[taskKey] == nil {
				receiver.newTask(taskConfig)
			}
		}
	}

	// 删除任务
	for oldTaskKey, oldTask := range receiver.taskList {
		if taskConfigListMap[oldTaskKey] == nil {
			oldTask.cancelFunc()
			delete(receiver.taskList, oldTaskKey)
			fmt.Printf("logAgent task exit success, topic:  %v \n", oldTask.config.Topic)
		}
	}
}

func (receiver *LogAgent) newTask(taskConfig *taskConfig) {
	taskKey := receiver.getTaskKey(taskConfig.FilePath, taskConfig.Topic)
	ctx, cancelFunc := context.WithCancel(context.Background())
	receiver.taskList[taskKey] = &task{
		tailLogObj: taillog.NewClient(taskConfig.FilePath),
		ctx:        ctx,
		cancelFunc: cancelFunc,
		config:     taskConfig,
	}

	go receiver.taskRun(receiver.taskList[taskKey])
}

func (receiver *LogAgent) taskRun(task *task)  {
	for {
		select {
		case <-task.ctx.Done() :
			return

		case logLine := <-task.tailLogObj.GetLogChan() :
			receiver.kafkaProducer.Push(task.config.Topic, logLine.Text)
		}
	}
}

func (receiver *LogAgent) getTaskKey(logPath, topic string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s_%s", logPath, topic))))[8:24]
}