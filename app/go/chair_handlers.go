package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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

	w.WriteHeader(http.StatusNoContent)
}

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

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	now := time.Now()

	chairLocationID := ulid.Make().String()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (?, ?, ?, ?, ?)`,
		chairLocationID, chair.ID, req.Latitude, req.Longitude, now,
	); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	location := &ChairLocation{}
	if err := tx.GetContext(ctx, location, `SELECT * FROM chair_locations WHERE id = ?`, chairLocationID); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	newStatus := ""
	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
	} else {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "PICKUP"); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = "PICKUP"
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ARRIVED"); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = "ARRIVED"
			}
		}
	}

	chairLocationCache.Update(chair.ID, func(cl *chairLocation) (*chairLocation, bool) {
		if cl == nil {
			return &chairLocation{
				TotalDistance:          0,
				LastLatitude:           req.Latitude,
				LastLongitude:          req.Longitude,
				TotalDistanceUpdatedAt: time.Now(),
			}, true
		}
		return &chairLocation{
			TotalDistance:          cl.TotalDistance + distance(cl.LastLatitude, cl.LastLongitude, req.Latitude, req.Longitude),
			LastLatitude:           req.Latitude,
			LastLongitude:          req.Longitude,
			TotalDistanceUpdatedAt: time.Now(),
		}, true
	})

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	rideStatusesCache.Forget(ride.ID)
	if newStatus != "" {
		Publish(ride.ID, &RideEvent{
			status: newStatus,
		})
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: location.CreatedAt.UnixMilli(),
	})
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
	slog.Info("Sent notification to chair1",
		slog.String("ride_id", response.RideID),
		slog.String("chair_id", chair.ID),
		slog.String("user_id", response.User.ID),
		slog.String("status", response.Status),
		slog.String("response", sb.String()),
	)

	_, err = db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, status.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	ch := make(chan *RideEvent, 100)
	Subscribe(ride.ID, ch)
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			if event.status == "MATCHED" {
				continue
			}

			status, err := getLatestRideStatusWithID(ctx, db, ride.ID)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}

			response.Status = event.status

			sb := &strings.Builder{}
			err = json.NewEncoder(sb).Encode(response)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to encode response: %w", err))
				return
			}
			fmt.Fprintf(w, "data: %s\n", sb.String())
			flusher.Flush()
			slog.Info("Sent notification to chair2",
				slog.String("ride_id", response.RideID),
				slog.String("chair_id", chair.ID),
				slog.String("user_id", response.User.ID),
				slog.String("status", response.Status),
				slog.String("response", sb.String()),
			)

			_, err = db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, status.ID)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}

			if event.status == "COMPLETED" {
				return
			}
		}
	}
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
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

	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ENROUTE"); err != nil {
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
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "CARRYING"); err != nil {
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
	rideStatusesCache.Forget(ride.ID)

	Publish(ride.ID, &RideEvent{
		status: req.Status,
	})

	w.WriteHeader(http.StatusNoContent)
}
