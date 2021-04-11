package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// 基于sarama第三方库开发的kafka client

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	//config.Producer.Idempotent = true
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接kafka，新建一个同步生产者
	client, err := sarama.NewSyncProducer([]string{"192.168.204.129:9092","192.168.204.130:9092","192.168.204.131:9092"}, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer client.Close()

	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "messagetest"
	msg.Key = sarama.StringEncoder("miles")
	msg.Value = sarama.StringEncoder("this is a test log")

	// 发送消息
	for{
		partition, offset, err := client.SendMessage(msg)
		if err != nil {
			fmt.Println("send msg failed, err:", err)
			return
		}
		fmt.Printf("partition:%v offset:%v\n", partition, offset)
	}
}