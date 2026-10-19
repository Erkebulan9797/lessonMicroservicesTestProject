package notify

import (
	"context"
	"fmt"
	"log"
	"strings"

	"debit-notifier/internal/model"
)

type ContactGetter interface {
	Contact(ctx context.Context, id string) (model.Contact, error)
}

type Channel interface {
	CanSend(c model.Contact) bool
	Send(c model.Contact, text string) error
}

type Service struct {
	contacts ContactGetter
	channels []Channel
}

func New(contacts ContactGetter, channels ...Channel) *Service {
	return &Service{contacts: contacts, channels: channels}
}

func (s *Service) NotifyDebit(ctx context.Context, e model.DebitEvent) error {
	c, err := s.contacts.Contact(ctx, e.AccountID)
	if err != nil {
		return err
	}

	text := buildText(e)
	sent := 0
	for _, ch := range s.channels {
		if !ch.CanSend(c) {
			continue
		}
		if err := ch.Send(c, text); err != nil {
			return err
		}
		sent++
	}

	if sent == 0 {
		log.Printf("у счёта %s нет контактов, уведомление не ушло", e.AccountID)
	}
	return nil
}

func buildText(e model.DebitEvent) string {
	text := fmt.Sprintf("Списание %s %s со счёта %s. Остаток: %s %s.",
		money(e.Amount), e.Currency, e.AccountID, money(e.BalanceAfter), e.Currency)
	if strings.TrimSpace(e.Description) != "" {
		text += " " + e.Description
	}
	return text
}

func money(minor int64) string {
	return fmt.Sprintf("%d.%02d", minor/100, minor%100)
}
