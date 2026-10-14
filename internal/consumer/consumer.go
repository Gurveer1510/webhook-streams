package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Consumer struct {
	StreamName     string
	ConsumerGoup   string
	ConsumerPrefix string
	Concurrency    int
	Rdb            *redis.Client
	InstanceId     string
}

func NewConsumer(stream, goupName, prefix, instance string, noOfWorkers int, r *redis.Client) *Consumer {
	return &Consumer{
		StreamName:     stream,
		ConsumerGoup:   goupName,
		ConsumerPrefix: prefix,
		Concurrency:    noOfWorkers,
		InstanceId:     instance,
		Rdb:            r,
	}
}

func (c *Consumer) StartConsumerGroup() {
	var wg sync.WaitGroup

	for i := 1; i <= c.Concurrency; i++ {
		consumerName := fmt.Sprintf("%v%x-%d", c.ConsumerPrefix, c.InstanceId, i)

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			c.StartWorker(name)
		}(consumerName)
	}

	wg.Wait()
}

func (c *Consumer) StartWorker(workerName string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	cursor := "0-0"

	for {
		select {
		case <-ticker.C:
			nextCursor := c.RecoverWebhook(workerName, cursor)
			cursor = nextCursor
		default:
			streams, err := c.Rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
				Group:    c.ConsumerGoup,
				Consumer: workerName,
				Streams:  []string{c.StreamName, ">"},
				Block:    5 * time.Second,
				Count:    1,
			}).Result()

			if err == redis.Nil {
				continue
			} else if err != nil {
				fmt.Printf("error : xreadgroup : %v", err)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {

					raw, ok := msg.Values["payload"].(string)
					if !ok {
						fmt.Println("Invalid payload type")
						continue
					}
					err := ProcessMessage(raw)

					if err != nil {
						fmt.Println("Error processing payload:", err)
						continue
					}
					err = c.Rdb.XAck(context.Background(), c.StreamName, c.ConsumerGoup, msg.ID).Err()
					if err != nil {
						fmt.Println("XACK: failed:", err)
					}
				}
			}

		}
	}
}
