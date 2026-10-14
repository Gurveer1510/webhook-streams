package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func (c *Consumer) RecoverWebhook(worker, cursor string) string {

	messages, nextCursor, err := c.Rdb.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Stream:   c.StreamName,
		Group:    c.ConsumerGoup,
		Consumer: worker,
		Count:    1,
		MinIdle:  60 * time.Second,
		Start:    cursor,
	}).Result()
	if err != nil {
		log.Printf("XAUTOCLAIM failed: %v", err)
		return cursor
	}
	cursor = nextCursor
	for _, msg := range messages {
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
			log.Printf("XACK failed: %v", err)
			return cursor
		}
	}
	return cursor
}
