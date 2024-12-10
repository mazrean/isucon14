package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

const badgerDir = "../badger/"

var badgerDB *badger.DB

func initBadger() error {
	if badgerDB != nil {
		badgerDB.Close()
	}

	err := os.RemoveAll(badgerDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read badger directory: %w", err)
	}

	err = os.MkdirAll(badgerDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create badger directory: %w", err)
	}

	badgerDB, err = badger.Open(badger.DefaultOptions(badgerDir))
	if err != nil {
		return fmt.Errorf("failed to open badger: %w", err)
	}

	var chairLocations []struct {
		ChairID   string    `db:"chair_id"`
		TotalDist int       `db:"total_distance"`
		UpdatedAt time.Time `db:"total_distance_updated_at"`
	}
	if err := db.Select(&chairLocations, `SELECT chair_id,
		SUM(IFNULL(distance, 0)) AS total_distance,
		MAX(created_at)          AS total_distance_updated_at
	FROM (SELECT chair_id,
			created_at,
			ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
			ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
		FROM chair_locations) tmp
		GROUP BY chair_id`); err != nil {
		return fmt.Errorf("failed to select chair locations: %w", err)
	}

	var chairLatestLocations []struct {
		ChairID       string `db:"chair_id"`
		LastLatitude  int    `db:"latitude"`
		LastLongitude int    `db:"longitude"`
	}
	if err := db.Select(&chairLatestLocations, `SELECT cl.chair_id,
		cl.latitude,
		cl.longitude
	FROM chair_locations cl
	JOIN (SELECT chair_id, MAX(created_at) AS created_at
		FROM chair_locations
		GROUP BY chair_id) cl2
	ON cl.chair_id = cl2.chair_id AND cl.created_at = cl2.created_at`); err != nil {
		return fmt.Errorf("failed to select chair latest locations: %w", err)
	}

	chairLatestLocationMap := make(map[string]Coordinate)
	for _, loc := range chairLatestLocations {
		chairLatestLocationMap[loc.ChairID] = Coordinate{
			Latitude:  loc.LastLatitude,
			Longitude: loc.LastLongitude,
		}
	}
	err = badgerDB.Update(func(txn *badger.Txn) error {
		for _, loc := range chairLocations {
			bytesChairID := append([]byte("location"), []byte(loc.ChairID)...)
			buf := bytes.NewBuffer(nil)
			err := gob.NewEncoder(buf).Encode(chairLocation{
				TotalDistance:          loc.TotalDist,
				LastLatitude:           chairLatestLocationMap[loc.ChairID].Latitude,
				LastLongitude:          chairLatestLocationMap[loc.ChairID].Longitude,
				TotalDistanceUpdatedAt: loc.UpdatedAt,
			})
			if err != nil {
				return fmt.Errorf("failed to encode one time token: %w", err)
			}

			err = txn.Set(bytesChairID, buf.Bytes())
			if err != nil {
				return fmt.Errorf("failed to set one time token: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update badger: %w", err)
	}

	return nil
}

func getChairLocationFromBadger(chairID string) (*chairLocation, bool, error) {
	var (
		location chairLocation
		ok       bool
	)
	err := badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(chairID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			ok = false
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get item: %w", err)
		}

		ok = true
		err = item.Value(func(val []byte) error {
			buf := bytes.NewBuffer(val)
			err := gob.NewDecoder(buf).Decode(&location)
			if err != nil {
				return fmt.Errorf("failed to decode: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to view badger: %w", err)
	}

	return &location, ok, nil
}

func updateChairLocationToBadger(chairID string, coodinate *Coordinate) error {
	err := badgerDB.Update(func(txn *badger.Txn) error {
		bytesChairID := append([]byte("location"), []byte(chairID)...)
		item, err := txn.Get(bytesChairID)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("failed to get item: %w", err)
		}

		var location chairLocation
		if errors.Is(err, badger.ErrKeyNotFound) {
			location = chairLocation{
				TotalDistance:          0,
				LastLatitude:           coodinate.Latitude,
				LastLongitude:          coodinate.Longitude,
				TotalDistanceUpdatedAt: time.Now(),
			}
		} else {
			err = item.Value(func(val []byte) error {
				buf := bytes.NewBuffer(val)
				err := gob.NewDecoder(buf).Decode(&location)
				if err != nil {
					return fmt.Errorf("failed to decode: %w", err)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value: %w", err)
			}

			location.TotalDistance += distance(location.LastLatitude, location.LastLongitude, coodinate.Latitude, coodinate.Longitude)
			location.LastLatitude = coodinate.Latitude
			location.LastLongitude = coodinate.Longitude
			location.TotalDistanceUpdatedAt = time.Now()
		}

		buf := bytes.NewBuffer(nil)
		err = gob.NewEncoder(buf).Encode(location)
		if err != nil {
			return fmt.Errorf("failed to encode one time token: %w", err)
		}

		err = txn.Set(bytesChairID, buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to set one time token: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update badger: %w", err)
	}

	return nil
}
