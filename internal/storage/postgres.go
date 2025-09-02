package storage

import (
	"database/sql"
	"errors"
	"log"
)

// Storage — обёртка над *sql.DB
type Storage struct {
	DB *sql.DB
}

// SaveOrder сохраняет заказ и связанные данные в БД
func (s *Storage) SaveOrder(o Order) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// --- orders ---
	_, err = tx.Exec(`INSERT INTO orders 
		(order_uid, track_number, entry, locale, internal_signature, customer_id,
		 delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO NOTHING`,
		o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature,
		o.CustomerID, o.DeliveryService, o.Shardkey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil {
		return err
	}

	// --- deliveries ---
	_, err = tx.Exec(`INSERT INTO deliveries 
		(order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip,
		o.Delivery.City, o.Delivery.Address, o.Delivery.Region, o.Delivery.Email)
	if err != nil {
		return err
	}

	// --- payments ---
	_, err = tx.Exec(`INSERT INTO payments
		(order_uid, transaction, request_id, currency, provider, amount, payment_dt,
		 bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency,
		o.Payment.Provider, o.Payment.Amount, o.Payment.PaymentDT, o.Payment.Bank,
		o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee)
	if err != nil {
		return err
	}

	// --- items ---
	for _, it := range o.Items {
		_, err = tx.Exec(`INSERT INTO items
			(order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price,
			 nm_id, brand, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.Rid, it.Name,
			it.Sale, it.Size, it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	log.Printf("Order %s saved", o.OrderUID)
	return nil
}

// GetOrder достаёт заказ по UID из 4 таблиц
func (s *Storage) GetOrder(orderUID string) (Order, error) {
	var o Order

	// --- orders ---
	row := s.DB.QueryRow(`SELECT order_uid, track_number, entry, locale,
		internal_signature, customer_id, delivery_service, shardkey, sm_id,
		date_created, oof_shard
		FROM orders WHERE order_uid=$1`, orderUID)

	err := row.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale,
		&o.InternalSignature, &o.CustomerID, &o.DeliveryService, &o.Shardkey,
		&o.SmID, &o.DateCreated, &o.OofShard)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return o, errors.New("order not found")
		}
		return o, err
	}

	// --- deliveries ---
	row = s.DB.QueryRow(`SELECT name, phone, zip, city, address, region, email
		FROM deliveries WHERE order_uid=$1`, orderUID)
	err = row.Scan(&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip,
		&o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return o, err
	}

	// --- payments ---
	row = s.DB.QueryRow(`SELECT transaction, request_id, currency, provider, amount,
		payment_dt, bank, delivery_cost, goods_total, custom_fee
		FROM payments WHERE order_uid=$1`, orderUID)
	err = row.Scan(&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency,
		&o.Payment.Provider, &o.Payment.Amount, &o.Payment.PaymentDT, &o.Payment.Bank,
		&o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return o, err
	}

	// --- items ---
	rows, err := s.DB.Query(`SELECT chrt_id, track_number, price, rid, name,
		sale, size, total_price, nm_id, brand, status
		FROM items WHERE order_uid=$1`, orderUID)
	if err != nil {
		return o, err
	}
	defer rows.Close()

	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid,
			&it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			return o, err
		}
		o.Items = append(o.Items, it)
	}

	return o, nil
}

// GetAllOrders возвращает все заказы с вложенными сущностями.
func (s *Storage) GetAllOrders() ([]Order, error) {
	rows, err := s.DB.Query(`
		SELECT order_uid, track_number, entry, locale, internal_signature,
		       customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
		FROM orders
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(
			&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
			&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard,
		); err != nil {
			return nil, err
		}

		// delivery (1:1)
		if err := s.DB.QueryRow(`
			SELECT name, phone, zip, city, address, region, email
			FROM deliveries WHERE order_uid = $1
		`, o.OrderUID).Scan(
			&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City,
			&o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email,
		); err != nil {
			return nil, err
		}

		// payment (1:1)
		if err := s.DB.QueryRow(`
			SELECT transaction, request_id, currency, provider, amount, payment_dt, bank,
			       delivery_cost, goods_total, custom_fee
			FROM payments WHERE order_uid = $1
		`, o.OrderUID).Scan(
			&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider,
			&o.Payment.Amount, &o.Payment.PaymentDT, &o.Payment.Bank,
			&o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee,
		); err != nil {
			return nil, err
		}

		// items (1:N)
		itemRows, err := s.DB.Query(`
			SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status
			FROM items WHERE order_uid = $1
		`, o.OrderUID)
		if err != nil {
			return nil, err
		}
		for itemRows.Next() {
			var it Item
			if err := itemRows.Scan(
				&it.ChrtID, &it.TrackNumber, &it.Price, &it.Rid, &it.Name,
				&it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status,
			); err != nil {
				itemRows.Close()
				return nil, err
			}
			o.Items = append(o.Items, it)
		}
		itemRows.Close()

		orders = append(orders, o)
	}
	return orders, rows.Err()
}
