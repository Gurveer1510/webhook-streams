package httpclient

import (
	"io"
	"net/http"
	"time"
)

type Client struct {
	Client http.Client
}

func NewClient() *Client {
	client := http.Client{
		Timeout: 10 * time.Second,
	}

	return &Client{
		Client: client,
	}
}

func (c *Client) MakeRequest(method, target string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.Client.Do(req)
}
