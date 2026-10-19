package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"debit-notifier/internal/account"
	"debit-notifier/internal/events"
	"debit-notifier/internal/model"
)

type Handler struct {
	accounts *account.Store
	producer *events.Producer
}

func New(a *account.Store, p *events.Producer) http.Handler {
	h := &Handler{accounts: a, producer: p}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/debit", h.debit)
	mux.HandleFunc("POST /api/accounts/{id}/topup", h.topUp)
	mux.HandleFunc("GET /api/accounts/{id}/balance", h.balance)
	mux.HandleFunc("PUT /api/accounts/{id}/contact", h.contact)
	return mux
}

func (h *Handler) debit(w http.ResponseWriter, r *http.Request) {
	var req model.DebitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "неверный json")
		return
	}
	if req.AccountID == "" || req.Amount <= 0 {
		writeErr(w, http.StatusBadRequest, "нужны accountId и amount > 0")
		return
	}
	if req.Currency == "" {
		req.Currency = "KGS"
	}

	left, err := h.accounts.Debit(r.Context(), req.AccountID, req.Amount)
	if errors.Is(err, account.ErrNotEnoughMoney) {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err != nil {
		log.Println("debit:", err)
		writeErr(w, http.StatusInternalServerError, "ошибка списания")
		return
	}

	event := model.DebitEvent{
		EventID:      newID(),
		AccountID:    req.AccountID,
		Amount:       req.Amount,
		BalanceAfter: left,
		Currency:     req.Currency,
		Description:  req.Description,
		CreatedAt:    time.Now().UTC(),
	}

	if err := h.producer.Send(r.Context(), event); err != nil {
		// кафка недоступна - возвращаем деньги, чтобы не потерять уведомление молча
		log.Println("kafka send:", err)
		if _, rerr := h.accounts.TopUp(r.Context(), req.AccountID, req.Amount); rerr != nil {
			log.Println("откат не удался:", rerr)
		}
		writeErr(w, http.StatusBadGateway, "не удалось отправить событие, списание отменено")
		return
	}

	writeJSON(w, http.StatusOK, event)
}

func (h *Handler) topUp(w http.ResponseWriter, r *http.Request) {
	amount, err := strconv.ParseInt(r.URL.Query().Get("amount"), 10, 64)
	if err != nil || amount <= 0 {
		writeErr(w, http.StatusBadRequest, "amount должен быть положительным числом")
		return
	}
	bal, err := h.accounts.TopUp(r.Context(), r.PathValue("id"), amount)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "ошибка redis")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"balance": bal})
}

func (h *Handler) balance(w http.ResponseWriter, r *http.Request) {
	bal, err := h.accounts.Balance(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "ошибка redis")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"balance": bal})
}

func (h *Handler) contact(w http.ResponseWriter, r *http.Request) {
	var c model.Contact
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeErr(w, http.StatusBadRequest, "неверный json")
		return
	}
	if err := h.accounts.SaveContact(r.Context(), r.PathValue("id"), c); err != nil {
		writeErr(w, http.StatusInternalServerError, "ошибка redis")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
