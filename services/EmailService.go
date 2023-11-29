package services

import (
	"context"
	"os"

	"github.com/trycourier/courier-go/v2"
)

type EmailService struct {
}

func (service *EmailService) SendNotifications(email string, data map[string]string) error {
	client := courier.CreateClient(string(os.Getenv("COURIER_KEY")), nil)
	_, err := client.SendMessage(
		context.Background(),
		courier.SendMessageRequestBody{
			Message: map[string]interface{}{
				"to": map[string]string{
					"email": email,
				},
				"template": os.Getenv("FORGET_PASSWORD_TEMPLATE_ID"),
				"data":     data,
			},
		},
	)

	if err != nil {
		return err
	}
	return nil
}
