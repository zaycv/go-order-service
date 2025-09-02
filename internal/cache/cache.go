package cache

import (
	"log"
	"sync"

	"github.com/zaycv/go-order-service/internal/storage"
)

type Cache struct {
	mu     sync.RWMutex
	orders map[string]storage.Order
}

func NewCache() *Cache {
	return &Cache{
		orders: make(map[string]storage.Order),
	}
}

// Получить заказ из кеша
func (c *Cache) Get(orderUID string) (storage.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	o, ok := c.orders[orderUID]
	return o, ok
}

// Сохранить заказ в кеш
func (c *Cache) Set(o storage.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orders[o.OrderUID] = o
}

// Инициализация кеша из БД
func (c *Cache) LoadFromDB(s *storage.Storage) error {
	orders, err := s.GetAllOrders()
	if err != nil {
		return err
	}
	c.mu.Lock()
	for _, o := range orders {
		c.orders[o.OrderUID] = o
	}
	count := len(orders)
	c.mu.Unlock()

	log.Printf("cache warmed: %d orders", count)
	return nil
}
