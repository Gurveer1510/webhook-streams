package usecase

import (
	"context"
	"encoding/json"
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

func (w *WebhookUsecase) WebhookUsecase(ctx context.Context, wh core.Webhook) (string, error) {
	byteData, err := json.Marshal(wh)
	if err != nil {
		return "", err
	}
	id, err := w.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: core.WebhookStream,
		Values: map[string]interface{}{
			"payload": string(byteData),
		},
	}).Result()

	if err != nil {
		return "", err
	}
	return id, nil
}
