package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"webhookstream/internal/core"
	"webhookstream/internal/usecase"

	"github.com/go-playground/validator/v10"
)

type Handler struct {
	whUsecase *usecase.WebhookUsecase
	validator *validator.Validate
}

func NewHandler(whU *usecase.WebhookUsecase) *Handler {
	return &Handler{
		whUsecase: whU,
		validator: validator.New(
			validator.WithRequiredStructEnabled()),
	}
}

func (h *Handler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	var whPayload core.Webhook

	if err := DecodeAndValidate(r, &whPayload, h.validator); err != nil {
		// Convert validation and decoding errors to HTTP responses
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	id, err := h.whUsecase.WebhookUsecase(r.Context(), whPayload)

	if err != nil {
		http.Error(w, "error while adding to redis", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"message":   "webhook queued",
		"stream_id": id,
	})
}

func DecodeAndValidate(
	r *http.Request,
	dst any,
	v *validator.Validate,
) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return err
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}

	return v.Struct(dst)
}
