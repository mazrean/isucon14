package main

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger"
	isucache "github.com/mazrean/isucon-go-tools/v2/cache"
	"github.com/oklog/ulid/v2"
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

			err = txn.Set(bytesChairID, encodeChairLocation(&chairLocation{
				TotalDistance:          loc.TotalDist,
				LastLatitude:           chairLatestLocationMap[loc.ChairID].Latitude,
				LastLongitude:          chairLatestLocationMap[loc.ChairID].Longitude,
				TotalDistanceUpdatedAt: loc.UpdatedAt.UnixMilli(),
			}))
			if err != nil {
				return fmt.Errorf("failed to set one time token: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update badger: %w", err)
	}

	chairStatusMap := make(map[string]chairStatus)
	userStatusMap := make(map[string]bool)

	users := []User{}
	if err := db.Select(&users, "SELECT * FROM users"); err != nil {
		return fmt.Errorf("failed to select users: %w", err)
	}

	for _, user := range users {
		userStatusMap[user.ID] = false
	}

	chairs := []Chair{}
	if err := db.Select(&chairs, "SELECT * FROM chairs"); err != nil {
		return fmt.Errorf("failed to select chairs: %w", err)
	}
	for _, chair := range chairs {
		chairStatusMap[chair.ID] = chairStatus{
			status: chairStatusAvailable,
			rideID: ulid.Make().String(),
		}
	}

	var rideStatuses []struct {
		RideID  string         `db:"id"`
		Status  string         `db:"status"`
		ChairID sql.NullString `db:"chair_id"`
		UserID  string         `db:"user_id"`
		IsSent  bool           `db:"is_sent"`
	}
	if err := db.Select(&rideStatuses, "SELECT ride_status.status, rides.chair_id, rides.id, rides.user_id, (ride_status.chair_sent_at IS NOT NULL) as is_sent FROM ride_statuses as ride_status JOIN rides ON ride_status.ride_id = rides.id WHERE ride_status.created_at = (SELECT MAX(rs.created_at) FROM ride_statuses as rs WHERE ride_status.ride_id = rs.ride_id) ORDER BY ride_status.created_at"); err != nil {
		return err
	}
	for _, status := range rideStatuses {
		userStatusMap[status.UserID] = status.Status != "COMPLETED"

		if !status.ChairID.Valid {
			continue
		}

		statusByte := chairStatusAvailable
		switch status.Status {
		case "MATCHING":
			statusByte = chairStatusMatched
		case "ENROUTE":
			statusByte = chairStatusEnRoute
		case "PICKUP":
			statusByte = chairStatusPickup
		case "CARRYING":
			statusByte = chairStatusCarrying
		case "ARRIVED":
			statusByte = chairStatusArrived
		case "COMPLETED":
			if status.IsSent {
				statusByte = chairStatusAvailable
			} else {
				statusByte = chairStatusCompleted
			}
		}

		chairStatusMap[status.ChairID.String] = chairStatus{
			status: statusByte,
			rideID: status.RideID,
		}
	}

	err = badgerDB.Update(func(txn *badger.Txn) error {
		for chairID, status := range chairStatusMap {
			bytesChairID := append([]byte("status"), []byte(chairID)...)
			err = txn.Set(bytesChairID, encodeChairStatus(&status))
			if err != nil {
				return fmt.Errorf("failed to set one time token: %w", err)
			}
		}

		for userID, status := range userStatusMap {
			bytesUserID := append([]byte("user"), []byte(userID)...)
			data := []byte{0}
			if status {
				data[0] = 1
			}
			err = txn.Set(bytesUserID, data)
			if err != nil {
				return fmt.Errorf("failed to set one time token: %w", err)
			}
		}

		return nil
	})

	return nil
}

type chairLocation struct {
	TotalDistance          int   `db:"total_distance"`
	LastLatitude           int   `db:"last_latitude"`
	LastLongitude          int   `db:"last_longitude"`
	TotalDistanceUpdatedAt int64 `db:"total_distance_updated_at"`
}

func encodeChairLocation(location *chairLocation) []byte {
	data := make([]byte, 32)
	binary.LittleEndian.PutUint64(data[:8], uint64(location.TotalDistance))
	binary.LittleEndian.PutUint64(data[8:16], uint64(location.LastLatitude))
	binary.LittleEndian.PutUint64(data[16:24], uint64(location.LastLongitude))
	binary.LittleEndian.PutUint64(data[24:32], uint64(location.TotalDistanceUpdatedAt))

	return data
}

func decodeChairLocation(data []byte) chairLocation {
	var location chairLocation
	location.TotalDistance = int(binary.LittleEndian.Uint64(data[:8]))
	location.LastLatitude = int(binary.LittleEndian.Uint64(data[8:16]))
	location.LastLongitude = int(binary.LittleEndian.Uint64(data[16:24]))
	location.TotalDistanceUpdatedAt = int64(binary.LittleEndian.Uint64(data[24:32]))

	return location
}

var (
	locationCache = isucache.NewAtomicMap[string, *chairLocation]("location")
)

func getChairLocationsFromBadger(chairIDs []string) (map[string]*chairLocation, error) {
	locations := make(map[string]*chairLocation, len(chairIDs))
	err := badgerDB.View(func(txn *badger.Txn) error {
		for _, chairID := range chairIDs {
			if location, ok := locationCache.Load(chairID); ok {
				locations[chairID] = location
				continue
			}

			bytesChairID := append([]byte("location"), []byte(chairID)...)
			item, err := txn.Get(bytesChairID)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to get item: %w", err)
			}

			err = item.Value(func(val []byte) error {
				location := decodeChairLocation(val)
				locations[chairID] = &location
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value: %w", err)
			}

			locationCache.Store(chairID, locations[chairID])
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to view badger: %w", err)
	}

	return locations, nil
}

func getChairLocationFromBadger(chairID string) (*chairLocation, bool, error) {
	if location, ok := locationCache.Load(chairID); ok {
		return location, true, nil
	}

	var (
		location chairLocation
		ok       bool
	)
	err := badgerDB.View(func(txn *badger.Txn) error {
		bytesChairID := append([]byte("location"), []byte(chairID)...)
		item, err := txn.Get(bytesChairID)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ok = false
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get item: %w", err)
		}

		ok = true
		err = item.Value(func(val []byte) error {
			location = decodeChairLocation(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}

		locationCache.Store(chairID, &location)
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
				TotalDistanceUpdatedAt: time.Now().UnixMilli(),
			}
		} else {
			err = item.Value(func(val []byte) error {
				location = decodeChairLocation(val)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get value: %w", err)
			}

			location.TotalDistance += distance(location.LastLatitude, location.LastLongitude, coodinate.Latitude, coodinate.Longitude)
			location.LastLatitude = coodinate.Latitude
			location.LastLongitude = coodinate.Longitude
			location.TotalDistanceUpdatedAt = time.Now().UnixMilli()
		}

		err = txn.Set(bytesChairID, encodeChairLocation(&location))
		if err != nil {
			return fmt.Errorf("failed to set one time token: %w", err)
		}
		locationCache.Store(chairID, &location)

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update badger: %w", err)
	}

	return nil
}

const (
	chairStatusAvailable byte = iota
	chairStatusMatched
	chairStatusEnRoute
	chairStatusPickup
	chairStatusCarrying
	chairStatusArrived
	chairStatusCompleted
)

type chairStatus struct {
	status byte
	rideID string
}

func encodeChairStatus(status *chairStatus) []byte {
	data := make([]byte, 1, 17)
	data[0] = status.status
	data = append(data, []byte(status.rideID)...)

	return data
}

func decodeChairStatus(data []byte) chairStatus {
	return chairStatus{
		status: data[0],
		rideID: string(data[1:]),
	}
}

func getChairStatusFromBadger(chairID string) (*chairStatus, bool, error) {
	var (
		status chairStatus
		ok     bool
	)
	err := badgerDB.View(func(txn *badger.Txn) error {
		bytesChairID := append([]byte("status"), []byte(chairID)...)
		item, err := txn.Get(bytesChairID)
		if errors.Is(err, badger.ErrKeyNotFound) {
			ok = false
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get item: %w", err)
		}

		ok = true
		err = item.Value(func(val []byte) error {
			status = decodeChairStatus(val)
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

	return &status, ok, nil
}

func updateChairStatusToBadger(chairID string, status *chairStatus) error {
	err := badgerDB.Update(func(txn *badger.Txn) error {
		bytesChairID := append([]byte("status"), []byte(chairID)...)
		err := txn.Set(bytesChairID, encodeChairStatus(status))
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

func getUserStatusFromBadger(userID string) (bool, error) {
	var status byte
	err := badgerDB.View(func(txn *badger.Txn) error {
		bytesUserID := append([]byte("user"), []byte(userID)...)
		item, err := txn.Get(bytesUserID)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get item: %w", err)
		}

		err = item.Value(func(val []byte) error {
			status = val[0]
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to view badger: %w", err)
	}

	return status == 1, nil
}

func updateUserStatusToBadger(userID string, status bool) error {
	err := badgerDB.Update(func(txn *badger.Txn) error {
		bytesUserID := append([]byte("user"), []byte(userID)...)
		data := []byte{0}
		if status {
			data[0] = 1
		}
		err := txn.Set(bytesUserID, data)
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
