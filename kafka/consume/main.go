package main

import (
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

var (
	wg sync.WaitGroup
	TOPIC = "messagetest"
	GROUP = "messagetest"
)

func main() {
	// 根据给定的代理地址和配置创建一个消费者
	conf := sarama.NewConfig()
	client,err := sarama.NewClient([]string{"192.168.204.129:9092","192.168.204.130:9092","192.168.204.131:9092"}, conf)
	if err != nil {
		log.Fatalln(err)
	}

	// create consume
	consumer,err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// create offset manager
	offsetManager,err := sarama.NewOffsetManagerFromClient(GROUP,client)
	if err != nil {
		panic(err)
	}

	//Partitions(topic):该方法返回了该topic的所有分区id
	partitionList, err := consumer.Partitions(TOPIC)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}

	for _, partition := range partitionList {
		wg.Add(1)
		go myconsume(wg, consumer, offsetManager, partition)
	}

	wg.Wait()
}

func myconsume(wg *sync.WaitGroup,c sarama.Consumer,om sarama.OffsetManager,partition int32){
	defer wg.Done()
	pom, err := om.ManagePartition(TOPIC, partition)
	if err != nil {
		log.Fatalln(err)
	}
	defer pom.Close()

	offset, _ := pom.NextOffset()
	if offset == -1 {
		offset = sarama.OffsetOldest
	}
	pc, err := c.ConsumePartition(TOPIC, partition, offset)
	if err != nil {
		log.Fatalln(err)
	}
	defer pc.Close()

	for msg := range pc.Messages() {
		log.Printf("topic: %v, partition: %v, offset: %v, key: %v, value: %v\n", msg.Topic,msg.Partition, msg.Offset,string(msg.Key),string(msg.Value))
		pom.MarkOffset(msg.Offset + 1, "")
	}
}