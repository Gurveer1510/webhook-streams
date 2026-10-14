package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
	"webhookstream/internal/db/redis"
	"webhookstream/internal/interface/rest/handler"
	"webhookstream/internal/interface/rest/router"
	"webhookstream/internal/usecase"
)

func main() {

	redis, err := redis.NewRedisClient()
	if err != nil {
		log.Fatalf("ERROR: %v", err.Error())
	}
	WebhookUsecase := usecase.NewWebhookUsecase(redis)
	WebHookHandler := handler.NewHandler(WebhookUsecase)

	router := router.NewRouter(*WebHookHandler)

	server := http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  5 * time.Second,
		Handler:      router,
	}

	fmt.Println("Server started at :8080")
	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("ERROR: %v", err.Error())
	}

}
