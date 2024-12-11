package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"

	isucache "github.com/mazrean/isucon-go-tools/v2/cache"
	isuhttp "github.com/mazrean/isucon-go-tools/v2/http"
	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, r, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, r, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	func() {
		if req.IsActive {
			var status struct {
				Status string `db:"status"`
				IsSend bool   `db:"is_send"`
			}
			if err := db.GetContext(ctx, &status, "SELECT status, (chair_sent_at IS NOT NULL) as is_send FROM rides JOIN ride_statuses ON rides.id = ride_statuses.ride_id WHERE chair_id = ? ORDER BY ride_statuses.created_at DESC LIMIT 1", chair.ID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					status.Status = "COMPLETED"
					status.IsSend = true
				} else {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}
			}

			if status.IsSend && status.Status == "COMPLETED" {
				emptyChairsLocker.Lock()
				defer emptyChairsLocker.Unlock()

				emptyChairs = append(emptyChairs, chair)
			}
		} else {
			emptyChairsLocker.Lock()
			defer emptyChairsLocker.Unlock()

			for i, c := range emptyChairs {
				if c.ID == chair.ID {
					emptyChairs = append(emptyChairs[:i], emptyChairs[i+1:]...)
					break
				}
			}
		}
	}()

	w.WriteHeader(http.StatusNoContent)
}

var latestRideCache = isucache.NewAtomicMap[string, *Ride]("latestRideCache")

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	go func() {
		updateChairLocationToBadger(chair.ID, &Coordinate{
			Latitude:  req.Latitude,
			Longitude: req.Longitude,
		})
	}()

	now := time.Now()

	var newStatus *RideStatus
	var (
		ride *Ride
		ok   bool
	)
	if ride, ok = latestRideCache.Load(chair.ID); ok {
		status, err := getLatestRideStatus(ctx, db, ride.ID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				statusID := ulid.Make().String()
				if _, err := db.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", statusID, ride.ID, "PICKUP"); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = &RideStatus{
					ID:     statusID,
					Status: "PICKUP",
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				statusID := ulid.Make().String()
				if _, err := db.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", statusID, ride.ID, "ARRIVED"); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = &RideStatus{
					ID:     statusID,
					Status: "ARRIVED",
				}
			}
		}
	}

	if newStatus != nil {
		rideStatusesCache.Store(ride.ID, newStatus)
		ChairPublish(chair.ID, &RideEvent{
			status: newStatus.Status,
			rideID: ride.ID,
		})
		UserPublish(ride.UserID, &RideEvent{
			status: newStatus.Status,
			rideID: ride.ID,
		})
	}

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusOK)
	sb := &strings.Builder{}
	sb.WriteString(`{"recorded_at":`)
	sb.WriteString(fmt.Sprint(now.UnixMilli()))
	sb.WriteString("}")
	_, err := io.Copy(w, strings.NewReader(sb.String()))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func distance(lat1, lon1, lat2, lon2 int) int {
	return int(abs(lat1-lat2) + abs(lon1-lon2))
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, errors.New("expected http.ResponseWriter to be an http.Flusher"))
		return
	}

	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	ride := &Ride{}

	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
				RetryAfterMs: 100,
			})
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	status, err := getLatestRideStatusWithID(ctx, db, ride.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	user := &User{}
	err = db.GetContext(ctx, user, "SELECT * FROM users WHERE id = ?", ride.UserID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	response := &chairGetNotificationResponseData{
		RideID: ride.ID,
		User: simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		},
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Status: status.Status,
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	sb := &strings.Builder{}
	err = json.NewEncoder(sb).Encode(response)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to encode response: %w", err))
		return
	}
	fmt.Fprintf(w, "data: %s\n", sb.String())
	flusher.Flush()

	_, err = db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, status.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	ch := make(chan *RideEvent, 100)
	ChairSubscribe(chair.ID, ch)
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			if event.status == "MATCHED" {
				if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, event.rideID); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
							RetryAfterMs: 100,
						})
						return
					}
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				status, err = getLatestRideStatusWithID(ctx, db, ride.ID)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				user := &User{}
				err = db.GetContext(ctx, user, "SELECT * FROM users WHERE id = ?", ride.UserID)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				response = &chairGetNotificationResponseData{
					RideID: ride.ID,
					User: simpleUser{
						ID:   user.ID,
						Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
					},
					PickupCoordinate: Coordinate{
						Latitude:  ride.PickupLatitude,
						Longitude: ride.PickupLongitude,
					},
					DestinationCoordinate: Coordinate{
						Latitude:  ride.DestinationLatitude,
						Longitude: ride.DestinationLongitude,
					},
					Status: status.Status,
				}
			} else {
				status, err = getLatestRideStatusWithID(ctx, db, ride.ID)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				response.Status = status.Status
			}

			sb := &strings.Builder{}
			err = json.NewEncoder(sb).Encode(response)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to encode response: %w", err))
				return
			}
			fmt.Fprintf(w, "data: %s\n", sb.String())
			flusher.Flush()

			_, err = db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, status.ID)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}

			if status.Status == "COMPLETED" {
				go func() {
					emptyChairsLocker.Lock()
					defer emptyChairsLocker.Unlock()

					emptyChairs = append(emptyChairs, chair)
				}()
			}
		}
	}
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	isuhttp.SetPath(r, "/api/chair/rides/{ride_id}/status")
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, r, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if ride.ChairID.String != chair.ID {
		writeError(w, r, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	statusID := ulid.Make().String()
	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", statusID, ride.ID, "ENROUTE"); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, r, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", statusID, ride.ID, "CARRYING"); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, r, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	rideStatusesCache.Store(ride.ID, &RideStatus{
		ID:     statusID,
		Status: req.Status,
	})

	ChairPublish(chair.ID, &RideEvent{
		status: req.Status,
		rideID: ride.ID,
	})
	UserPublish(ride.UserID, &RideEvent{
		status: req.Status,
		rideID: ride.ID,
	})

	w.WriteHeader(http.StatusNoContent)
}
