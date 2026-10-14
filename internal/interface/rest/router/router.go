package router

import (
	"net/http"
	"webhookstream/internal/interface/rest/handler"

	"github.com/gorilla/mux"
)

func NewRouter(webhookHandler handler.Handler) mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/webhook",webhookHandler.HandleWebhook).Methods(http.MethodPost)

	return *router
}