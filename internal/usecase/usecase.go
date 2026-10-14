package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"webhookstream/internal/core"

	"github.com/redis/go-redis/v9"
)

type WebhookUsecase struct {
	redis *redis.Client
}

func NewWebhookUsecase(r *redis.Client) *WebhookUsecase {
	return &WebhookUsecase{
		redis: r,
	}
}

func (w *WebhookUsecase) WebhookUsecase(ctx context.Context, wh core.Webhook) {
	byteData, err := json.Marshal(wh)
	if err != nil {
		fmt.Printf("ERROR: %v", err.Error())
	}
	res := w.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: core.WehbhookStream,
		Values: byteData,
	})

	if res.Err() != nil {
		fmt.Printf("ERROR: %v", err)
	}
	result, _ :=res.Result()
	fmt.Printf("RESULT: %v \n", result)
}
