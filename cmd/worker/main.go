package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"webhookstream/internal/core"
	redisConnector "webhookstream/internal/db/redis"
	"webhookstream/internal/utils/httpclient"

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

	instanceId := make([]byte, 16)
	if _, err := rand.Read(instanceId); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := 1; i <= concurrency; i++ {
		consumerName := fmt.Sprintf("worker-%x-%d", instanceId, i)

		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			startConsumer(name, rdb)
		}(consumerName)
	}

	wg.Wait()
}

func startConsumer(worker string, rdb *redis.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	cursor := "0-0"
	for {
		select {
		case <-ticker.C:
			messages, nextCursor, err := rdb.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
				Stream:   webhookStream,
				Group:    webhookGroup,
				Consumer: worker,
				Count:    1,
				MinIdle:  60 * time.Second,
				Start:    cursor,
			}).Result()
			if err != nil {
				fmt.Printf("error : xautoclaim : %v", err)
				continue
			}
			cursor = nextCursor
			for _, msg := range messages {
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
		default:
			streams, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
				Group:    webhookGroup,
				Consumer: worker,
				Streams:  []string{webhookStream, ">"},
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
}

func processPayload(webhookPayload core.Webhook) error {
	client := httpclient.NewClient()

	byteData, err := json.Marshal(webhookPayload)
	if err != nil {
		fmt.Println("Error marshaling payload:", err)
		return err
	}
	body := bytes.NewReader(byteData)

	resp, err := client.MakeRequest(http.MethodPost, webhookPayload.TargetUrl, body)

	if err != nil {
		fmt.Println("Error sending webhook:", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
