package core

type Webhook struct {
	EventType string  `json:"event_type,omitempty" validate:"required"`
	OrderId   string  `json:"order_id,omitempty" validate:"required"`
	Amount    float64 `json:"amount,omitempty" validate:"required"`
	TargetUrl string  `json:"target_url,omitempty" validate:"required"`
}

const (
	WebhookStream = "webhook:raw"
)