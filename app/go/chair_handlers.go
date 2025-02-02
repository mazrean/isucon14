package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

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

	chairStatusGauge.WithLabelValues("REGISTERED").Inc()

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
			status, ok, err := getChairStatusFromBadger(chair.ID)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}

			if !ok {
				if err := updateChairStatusToBadger(chair.ID, &chairStatus{
					status: chairStatusAvailable,
					rideID: ulid.Make().String(),
				}); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}
			}

			if status.status == chairStatusAvailable {
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

	if req.IsActive {
		chairStatusGauge.WithLabelValues("REGISTERED").Dec()
		chairStatusGauge.WithLabelValues("COMPLETED").Inc()
	}

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateRequest struct {
	chairID    string
	coordinate *Coordinate
}

var latestRideCache = isucache.NewAtomicMap[string, *Ride]("latestRideCache")

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	now := time.Now()

	eg := errgroup.Group{}

	eg.Go(func() error {
		return updateChairLocationToBadger(chair.ID, req)
	})

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
				if err := updateChairStatusToBadger(chair.ID, &chairStatus{
					status: chairStatusPickup,
					rideID: ride.ID,
				}); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = &RideStatus{
					Status: "PICKUP",
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if err := updateChairStatusToBadger(chair.ID, &chairStatus{
					status: chairStatusArrived,
					rideID: ride.ID,
				}); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				newStatus = &RideStatus{
					Status: "ARRIVED",
				}
			}
		}
	}

	if newStatus != nil {
		rideStatusesCache.Store(ride.ID, newStatus)
		ChairPublish(chair.ID, &RideEvent{
			status: newStatus.Status,
			ride:   ride,
		})
		UserPublish(ride.UserID, &RideEvent{
			status: newStatus.Status,
			ride:   ride,
		})
	}

	if err := eg.Wait(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"recorded_at":`))
	w.Write([]byte(fmt.Sprint(now.UnixMilli())))
	w.Write([]byte("}"))
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

func (nrd *chairGetNotificationResponseData) Encode() string {
	sb := &strings.Builder{}
	sb.WriteString(`{"ride_id":"`)
	sb.WriteString(nrd.RideID)
	sb.WriteString(`","user":{"id":"`)
	sb.WriteString(nrd.User.ID)
	sb.WriteString(`","name":"`)
	sb.WriteString(nrd.User.Name)
	sb.WriteString(`"},"pickup_coordinate":{"latitude":`)
	sb.WriteString(fmt.Sprint(nrd.PickupCoordinate.Latitude))
	sb.WriteString(`,"longitude":`)
	sb.WriteString(fmt.Sprint(nrd.PickupCoordinate.Longitude))
	sb.WriteString(`},"destination_coordinate":{"latitude":`)
	sb.WriteString(fmt.Sprint(nrd.DestinationCoordinate.Latitude))
	sb.WriteString(`,"longitude":`)
	sb.WriteString(fmt.Sprint(nrd.DestinationCoordinate.Longitude))
	sb.WriteString(`},"status":"`)
	sb.WriteString(nrd.Status)
	sb.WriteString(`"}`)
	return sb.String()
}

var appGetNotificationRes = []byte(`{"retry_after_ms":50}`)

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, errors.New("expected http.ResponseWriter to be an http.Flusher"))
		return
	}

	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	var (
		status   *RideStatus
		user     = &User{}
		response *chairGetNotificationResponseData
		err      error
	)
	ride, ok := latestRideCache.Load(chair.ID)
	if !ok {
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(appGetNotificationRes)
		return
	}

	status, err = getLatestRideStatusWithID(ctx, db, ride.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	user = &User{}
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

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	fmt.Fprintf(w, "data: %s\n\n", response.Encode())
	flusher.Flush()

	if err := updateChairStatusToBadger(chair.ID, &chairStatus{
		status: chairStatusAvailable,
		rideID: ride.ID,
	}); err != nil {
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
				ride = event.ride
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

			fmt.Fprintf(w, "data: %s\n\n", response.Encode())
			flusher.Flush()

			if err := updateChairStatusToBadger(chair.ID, &chairStatus{
				status: chairStatusAvailable,
				rideID: ride.ID,
			}); err != nil {
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

	ride := &Ride{}
	if err := db.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
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
		if err := updateChairStatusToBadger(chair.ID, &chairStatus{
			status: chairStatusEnRoute,
			rideID: ride.ID,
		}); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, db, ride.ID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, r, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if err := updateChairStatusToBadger(chair.ID, &chairStatus{
			status: chairStatusCarrying,
			rideID: ride.ID,
		}); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, r, http.StatusBadRequest, errors.New("invalid status"))
	}

	rideStatusesCache.Store(ride.ID, &RideStatus{
		ID:     statusID,
		Status: req.Status,
	})

	ChairPublish(chair.ID, &RideEvent{
		status: req.Status,
		ride:   ride,
	})
	UserPublish(ride.UserID, &RideEvent{
		status: req.Status,
		ride:   ride,
	})

	w.WriteHeader(http.StatusNoContent)
}
