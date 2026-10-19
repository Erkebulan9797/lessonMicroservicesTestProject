package model

import "time"

// суммы в минорных единицах (тыйыны), чтобы не мучиться с float
type DebitEvent struct {
	EventID      string    `json:"eventId"`
	AccountID    string    `json:"accountId"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balanceAfter"`
	Currency     string    `json:"currency"`
	Description  string    `json:"description"`
	CreatedAt    time.Time `json:"createdAt"`
}

type DebitRequest struct {
	AccountID   string `json:"accountId"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	Description string `json:"description"`
}

type Contact struct {
	Email string `json:"email"`
	Phone string `json:"phone"`
}
