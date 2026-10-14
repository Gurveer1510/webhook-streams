package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"webhookstream/internal/core"
	"webhookstream/internal/usecase"
)

type Handler struct {
	whUsecase *usecase.WebhookUsecase
}

func NewHandler(whU *usecase.WebhookUsecase) *Handler {
	return &Handler{
		whUsecase: whU,
	}
}

func (h *Handler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	var whPayload core.Webhook

	if err := json.NewDecoder(r.Body).Decode(&whPayload); err != nil {
		fmt.Printf("ERROR: %v", err.Error())
		http.Error(w, "error decoding payload", http.StatusBadRequest)
		return
	}

	h.whUsecase.WebhookUsecase(r.Context(), whPayload)
}
