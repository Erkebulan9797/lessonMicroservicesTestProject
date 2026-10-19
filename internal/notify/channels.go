package notify

import (
	"log"

	"debit-notifier/internal/model"
)

type Email struct{}

func (Email) CanSend(c model.Contact) bool { return c.Email != "" }

// потом тут будет net/smtp, пока лог
func (Email) Send(c model.Contact, text string) error {
	log.Printf("[EMAIL -> %s] %s", c.Email, text)
	return nil
}

type SMS struct{}

func (SMS) CanSend(c model.Contact) bool { return c.Phone != "" }

// здесь будет вызов sms-шлюза
func (SMS) Send(c model.Contact, text string) error {
	log.Printf("[SMS -> %s] %s", c.Phone, text)
	return nil
}
