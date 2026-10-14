package core

type Webhook struct {
	EventType string  `json:"event_type,omitempty"`
	OrderId   string  `json:"order_id,omitempty"`
	Amount    float64 `json:"amount,omitempty"`
	TargetUrl string  `json:"target_url,omitempty"`
}

const (
	WehbhookStream = "webhook:raw"
)