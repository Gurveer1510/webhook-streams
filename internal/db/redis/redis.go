package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient() *redis.Client {
	redis := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	pingCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res := redis.Ping(pingCtx)
	if res.Err() != nil {
		return nil
	}
	pong, _ := res.Result()
	fmt.Println("PING: ", pong)
	return redis
}
