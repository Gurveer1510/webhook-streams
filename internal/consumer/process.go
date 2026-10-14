package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"webhookstream/internal/core"
	"webhookstream/internal/utils/httpclient"
)

func ProcessMessage(message string) error {
	client := httpclient.NewClient()
	var payload core.Webhook
	if err := json.Unmarshal([]byte(message), &payload); err != nil {
		fmt.Println("Invalid JSON:", err)
		return err
	}
	byteData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("Error marshaling payload:", err)
		return err
	}
	body := bytes.NewReader(byteData)

	resp, err := client.MakeRequest(http.MethodPost, payload.TargetUrl, body)

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
