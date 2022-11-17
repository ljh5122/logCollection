package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"logCollection/pkg/config"
	"logCollection/pkg/elastic"
	"logCollection/pkg/etcd"
	"logCollection/pkg/kafka"
)


type LogTransfer struct {
	kafkaConsumer 	*kafka.Consumer
	etcdClient 		*etcd.Client
	elasticClient	*elastic.Elastic
	etcdConfigKey	string
	taskList 		map[string]*transferTask
}

type transferTask struct {
	ctx 			context.Context
	cancelFunc		context.CancelFunc
	config			*transferTaskConfig
}

type transferTaskConfig struct {
	FilePath 	string	`json:"file_path"`
	Topic		string	`json:"topic"`
}

type LogBody struct {
	Content string	`json:"content"`
}

func NewLogTransfer(appConfig *config.Config) *LogTransfer {
	return &LogTransfer{
		kafkaConsumer: kafka.NewConsumer(appConfig.Kafka.Addr),
		etcdClient:    etcd.NewClient(appConfig.Etcd.Addr),
		elasticClient: elastic.NewElastic(appConfig.Elastic.Addr, appConfig.Elastic.UserName, appConfig.Elastic.Password),
		etcdConfigKey: appConfig.Log.EtcdKey,
		taskList:      make(map[string]*transferTask, 8),
	}
}

func (receiver *LogTransfer) Run()  {
	resp := receiver.etcdClient.Get(receiver.etcdConfigKey)
	taskConfigList := make([]*transferTaskConfig, 8)
	for _, kv := range resp.Kvs {
		if err := json.Unmarshal(kv.Value, &taskConfigList); err != nil {
			fmt.Println("LogTransfer GetConfig fail, ", err)
		}
	}

	receiver.tidyTask(taskConfigList)
	go receiver.watchConfigFromEtcd()
}

func (receiver *LogTransfer) tidyTask(taskConfigList []*transferTaskConfig) {
	taskConfigListMap := make(map[string]*transferTaskConfig, 8)
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
			fmt.Printf("LogTransfer task exit success, topic:  %v \n", oldTask.config.Topic)
		}
	}
}

func (receiver *LogTransfer) newTask(taskConfig *transferTaskConfig) {
	taskKey := receiver.getTaskKey(taskConfig.FilePath, taskConfig.Topic)
	ctx, cancelFunc := context.WithCancel(context.Background())
	receiver.taskList[taskKey] = &transferTask{
		ctx: ctx,
		cancelFunc: cancelFunc,
		config: taskConfig,
	}

	receiver.taskRun(receiver.taskList[taskKey])
	fmt.Printf("LogTransfer newTask success, topic:  %v \n", taskConfig.Topic)
}

func (receiver *LogTransfer) taskRun(task *transferTask)  {
	receiver.kafkaConsumer.Consume(task.ctx, task.config.Topic, func(message *sarama.ConsumerMessage) {
		jsonStr, _ := json.Marshal(&LogBody{
			Content: string(message.Value),
		})

		receiver.elasticClient.Put(task.config.Topic, string(jsonStr))
	})
}

func (receiver *LogTransfer) watchConfigFromEtcd()  {
	watchCh := receiver.etcdClient.Watch(receiver.etcdConfigKey)
	taskConfigList := make([]*transferTaskConfig, 8)
	for WatchResponse := range watchCh {
		for _, event := range WatchResponse.Events {
			if event.Type == etcd.EventTypePut {
				if err := json.Unmarshal(event.Kv.Value, &taskConfigList); err != nil {
					fmt.Println("LogTransfer configs WatchConfigFromEtcd fail, ", err)
				}
			}

			receiver.tidyTask(taskConfigList)
		}
	}
}

func (receiver *LogTransfer) getTaskKey(logPath, topic string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%s_%s", logPath, topic))))[8:24]
}