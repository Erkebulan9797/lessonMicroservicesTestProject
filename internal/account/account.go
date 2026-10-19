package account

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"

	"debit-notifier/internal/model"
)

var ErrNotEnoughMoney = errors.New("недостаточно средств")

// списываем только если хватает, иначе -1. Всё одной операцией, гонок нет
var debitScript = redis.NewScript(`
local balance = tonumber(redis.call('GET', KEYS[1]) or '0')
local amount = tonumber(ARGV[1])
if balance < amount then
	return -1
end
return redis.call('DECRBY', KEYS[1], amount)
`)

type Store struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func (s *Store) TopUp(ctx context.Context, id string, amount int64) (int64, error) {
	return s.rdb.IncrBy(ctx, balanceKey(id), amount).Result()
}

func (s *Store) Balance(ctx context.Context, id string) (int64, error) {
	v, err := s.rdb.Get(ctx, balanceKey(id)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

// возвращает новый баланс
func (s *Store) Debit(ctx context.Context, id string, amount int64) (int64, error) {
	left, err := debitScript.Run(ctx, s.rdb, []string{balanceKey(id)}, amount).Int64()
	if err != nil {
		return 0, err
	}
	if left < 0 {
		return 0, ErrNotEnoughMoney
	}
	return left, nil
}

func (s *Store) SaveContact(ctx context.Context, id string, c model.Contact) error {
	fields := map[string]any{}
	if c.Email != "" {
		fields["email"] = c.Email
	}
	if c.Phone != "" {
		fields["phone"] = c.Phone
	}
	if len(fields) == 0 {
		return nil
	}
	return s.rdb.HSet(ctx, contactKey(id), fields).Err()
}

func (s *Store) Contact(ctx context.Context, id string) (model.Contact, error) {
	m, err := s.rdb.HGetAll(ctx, contactKey(id)).Result()
	if err != nil {
		return model.Contact{}, err
	}
	return model.Contact{Email: m["email"], Phone: m["phone"]}, nil
}

func balanceKey(id string) string { return "balance:" + id }
func contactKey(id string) string { return "contact:" + id }
