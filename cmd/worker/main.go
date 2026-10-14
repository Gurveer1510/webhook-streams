package main

import (
	"context"
	"crypto/rand"
	"errors"
	"log"
	"webhookstream/internal/consumer"
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

	instanceId := make([]byte, 16)
	if _, err := rand.Read(instanceId); err != nil {
		log.Fatal(err)
	}

	consumer := consumer.NewConsumer(webhookStream, webhookGroup, consumerPrefix, string(instanceId), concurrency, rdb)

	consumer.StartConsumerGroup()
}
