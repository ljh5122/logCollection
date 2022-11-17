package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type Producer struct {
	syncProducer sarama.SyncProducer
}

type Consumer struct {
	consumer sarama.Consumer
}

type consumerFunc func(message *sarama.ConsumerMessage)

func NewProducer(addr ...string) *Producer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	syncProducer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		fmt.Printf("kafka NewSyncProducer fail, err : %v", err)
	}

	return &Producer{syncProducer}
}

func (receiver *Producer) Push(topic string, message string) {
	_, _, err := receiver.syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	})

	if err != nil {
		fmt.Printf("SendMessage fail , err : %v \n", err)
		return
	}
}


func NewConsumer(addr ...string) *Consumer {
	consumer, err := sarama.NewConsumer(addr, sarama.NewConfig())
	if err != nil {
		fmt.Printf("kafka NewKafkaConsumer fail, err : %v \n", err)
	}

	return &Consumer{consumer}
}

func (receiver *Consumer) Consume(cxt context.Context,topic string, consumerCallFunc consumerFunc) {
	partitionList, err := receiver.consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("get Partitions from kafka fail, %v \n", err)
		return
	}

	for _, partition := range partitionList{
		pc, err := receiver.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("consume Partitions fail, %v \n", err)
			continue
		}

		go func(sarama.PartitionConsumer) {
			for {
				select {
				case <-cxt.Done() :
					pc.AsyncClose()
					return

				case message := <-pc.Messages() :
					consumerCallFunc(message)
				}
			}
		}(pc)
	}
}
