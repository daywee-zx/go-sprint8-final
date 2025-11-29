package main

import (
	"database/sql"
	"errors"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// p.Number is ignored
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) "+
		"VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	var (
		client    int
		status    string
		address   string
		createdAt string
	)

	row := s.db.QueryRow("SELECT client, status, address, created_at "+
		"FROM parcel WHERE number = :number", sql.Named("number", number))
	err := row.Scan(&client, &status, &address, &createdAt)
	if err != nil {
		return Parcel{}, err
	}

	p := Parcel{number, client, status, address, createdAt}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query("SELECT number, status, address, created_at "+
		"FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return nil, err
	}

	var res []Parcel
	for rows.Next() {
		var (
			num       int
			status    string
			address   string
			createdAt string
		)

		err = rows.Scan(&num, &status, &address, &createdAt)
		if err != nil {
			return nil, err
		}

		p := Parcel{num, client, status, address, createdAt}
		res = append(res, p)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("number", number),
		sql.Named("status", status))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	var status string

	row := s.db.QueryRow("SELECT status FROM parcel Where number = :number",
		sql.Named("number", number))
	err := row.Scan(&status)
	if err != nil {
		return err
	}

	if status != ParcelStatusRegistered {
		return errors.New("invalid status: parcel not registered")
	}

	_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number",
		sql.Named("address", address),
		sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	var status string

	row := s.db.QueryRow("SELECT status FROM parcel Where number = :number",
		sql.Named("number", number))
	err := row.Scan(&status)
	if err != nil {
		return err
	}

	if status != ParcelStatusRegistered {
		return errors.New("invalid status: parcel not registered")
	}

	_, err = s.db.Exec("DELETE FROM parcel WHERE number = :number",
		sql.Named("number", number))
	if err != nil {
		return err
	}

	return nil
}
