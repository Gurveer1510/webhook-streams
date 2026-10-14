package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
	"webhookstream/internal/core"
	redisConnector "webhookstream/internal/db/redis"

	"github.com/redis/go-redis/v9"
)

const (
	webhookStream  = "webhook:raw"
	webhookGroup   = "http-delivery"
	consumerPrefix = "worker-"
	concurrency    = 2
)

func main() {
	ctx := context.Background()

	rdb, err := redisConnector.NewRedisClient()
	if err != nil {
		log.Fatalf("error : connecting to redia : %v", err)
	}
	defer rdb.Close()

	err = rdb.XGroupCreateMkStream(ctx, webhookStream, webhookGroup, "0").Err()

	if err != nil && !errors.Is(err, redis.Nil) {
		if !redis.HasErrorPrefix(err, "BUSYGROUP") {
			log.Fatal(err)
		}
	}

	for i := 1; i <= concurrency; i++ {
		startConsumer(strconv.Itoa(i), rdb)
	}
}

func startConsumer(worker string, rdb *redis.Client) {
	for {
		streams, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    webhookGroup,
			Consumer: fmt.Sprintf("%v%v", consumerPrefix, worker),
			Streams:  []string{webhookStream, ">"},
			Block:    5 * time.Second,
			// Count: 2,
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
				var payload core.Webhook
				if err := json.Unmarshal([]byte(raw), &payload); err != nil {
					fmt.Println("Invalid JSON:", err)
					continue
				}

				err := processPayload(payload)
				if err != nil {
					fmt.Println("Error processing payload:", err)
					continue
				}
				err = rdb.XAck(context.Background(), webhookStream, webhookGroup, msg.ID).Err()
				if err != nil {
					fmt.Println("XACK: failed:", err)
				}
			}
		}
	}
}

func processPayload(webhookPayload core.Webhook) error{
	byteData, err := json.Marshal(webhookPayload)
	if err != nil {
		fmt.Println("Error marshaling payload:", err)
		return err
	}
	body := bytes.NewReader(byteData)
	resp, err := http.Post(webhookPayload.TargetUrl, "application/json", body)
	if err != nil {
		fmt.Println("Error sending webhook:", err)
		return err
	}
	defer resp.Body.Close()
	return err
}
