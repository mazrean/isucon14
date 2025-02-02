package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/goccy/go-json"
	"github.com/motoki317/sc"

	"github.com/jmoiron/sqlx"
	isucache "github.com/mazrean/isucon-go-tools/v2/cache"
	"github.com/oklog/ulid/v2"
)

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, r, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userID, req.Username, req.FirstName, req.LastName, req.DateOfBirth, accessToken, invitationCode,
	)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 初回登録キャンペーンのクーポンを付与
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
		userID, "CP_NEW2024", 3000,
	)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	// 招待コードを使った登録
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		// ユーザーチェック
		var inviter User
		err = tx.GetContext(ctx, &inviter, "SELECT * FROM users WHERE invitation_code = ?", *req.InvitationCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				return
			}
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		// 招待クーポン付与
		// 招待した人にもRewardを付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?), (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)",
			userID, "INV_"+*req.InvitationCode, 1500, inviter.ID, "RWD_"+*req.InvitationCode, 1000,
		)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		// 招待する側の招待数をチェック
		var coupons []Coupon
		err = tx.SelectContext(ctx, &coupons, "SELECT * FROM coupons WHERE code = ?", "INV_"+*req.InvitationCode)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if len(coupons) > 3 {
			writeError(w, r, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			return
		}

	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	accessTokenCache.Forget(accessToken)

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	userStatusGauge.WithLabelValues("COMPLETED").Inc()

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, r, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)`,
		user.ID,
		req.Token,
	)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	paymentTokenCache.Store(user.ID, &PaymentToken{
		UserID: user.ID,
		Token:  req.Token,
	})

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC FOR UPDATE`,
		user.ID,
	); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	// Collect all ride IDs
	rideIDs := make([]string, len(rides))
	for i, ride := range rides {
		rideIDs[i] = ride.ID
	}

	items := []getAppRidesResponseItem{}
	for _, ride := range rides {
		newRide, exists := rideCache.Load(ride.ID)
		if exists {
			ride = *newRide
		}

		status, exists := rideStatusesCache.Load(ride.ID)
		if !exists || status.Status != "COMPLETED" {
			continue
		}

		fare, err := calculateDiscountedFare(ctx, tx, user.ID, &ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		item := getAppRidesResponseItem{
			ID:                    ride.ID,
			PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
			DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
			Fare:                  fare,
			Evaluation:            *ride.Evaluation,
			RequestedAt:           ride.CreatedAt.UnixMilli(),
			CompletedAt:           ride.UpdatedAt.UnixMilli(),
		}

		item.Chair = getAppRidesResponseItemChair{}

		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		item.Chair.ID = chair.ID
		item.Chair.Name = chair.Name
		item.Chair.Model = chair.Model

		owner := &Owner{}
		if err := tx.GetContext(ctx, owner, `SELECT * FROM owners WHERE id = ?`, chair.OwnerID); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		item.Chair.Owner = owner.Name

		items = append(items, item)
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

type executableGet interface {
	Rebind(query string) string
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

var rideStatusesCache = isucache.NewAtomicMap[string, *RideStatus]("rideStatusesCache")

func initRideStatusesCache() error {
	var rides []Ride
	if err := db.Select(&rides, "SELECT * FROM rides"); err != nil {
		return err
	}

	for _, ride := range rides {
		if ride.ChairID.Valid {
			rideStatusesCache.Store(ride.ID, &RideStatus{
				RideID: ride.ID,
				Status: "COMPLETED",
			})
		} else {
			rideStatusesCache.Store(ride.ID, &RideStatus{
				RideID: ride.ID,
				Status: "MATCHING",
			})
		}
	}

	err := badgerDB.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte("status")
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			rideID := ulid.ULID(k[6:]).String()

			err := item.Value(func(v []byte) error {
				status := decodeChairStatus(v)
				strStatus := "MATCHING"
				switch status.status {
				case chairStatusMatched:
					strStatus = "MATCHING"
				case chairStatusEnRoute:
					strStatus = "ENROUTE"
				case chairStatusPickup:
					strStatus = "PICKUP"
				case chairStatusCarrying:
					strStatus = "CARRYING"
				case chairStatusArrived:
					strStatus = "ARRIVED"
				case chairStatusCompleted:
					strStatus = "COMPLETED"
				}

				rideStatusesCache.Store(rideID, &RideStatus{
					RideID: rideID,
					Status: strStatus,
				})

				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to get status: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func getLatestRideStatus(ctx context.Context, tx executableGet, rideID string) (string, error) {
	rideStatus, ok := rideStatusesCache.Load(rideID)
	if !ok {
		return "", sql.ErrNoRows
	}

	return rideStatus.Status, nil
}

func getLatestRideStatusWithID(ctx context.Context, tx executableGet, rideID string) (*RideStatus, error) {
	rideStatus, ok := rideStatusesCache.Load(rideID)
	if !ok {
		return nil, sql.ErrNoRows
	}

	return rideStatus, nil
}

// Modified appPostRides function with reduced SQL executions
func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, r, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	var l int
	func() {
		matchingRidesLock.RLock()
		defer matchingRidesLock.RUnlock()

		l = len(matchingRides)
	}()
	if l > 100 {
		time.Sleep(5000 * time.Millisecond)
	} else if l > 50 {
		time.Sleep(1000 * time.Millisecond)
	}
	now := time.Now()

	user := ctx.Value("user").(*User)
	rideID := ulid.Make().String()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// Replace fetching all rides and iterating with a single count query
	userStatus, err := getUserStatusFromBadger(user.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if userStatus {
		writeError(w, r, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude, created_at, updated_at)
				  VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rideID, user.ID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude, now, now,
	); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := updateUserStatusToBadger(user.ID, true); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var rideCount int
	if err := tx.GetContext(ctx, &rideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ? `, user.ID); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var coupon Coupon
	if rideCount == 1 {
		// 初回利用で、初回利用クーポンがあれば必ず使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}

			// 無ければ他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}
			} else {
				if _, err := tx.ExecContext(
					ctx,
					"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
					rideID, user.ID, coupon.Code,
				); err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = 'CP_NEW2024'",
				rideID, user.ID,
			); err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}
		}
	} else {
		// 他のクーポンを付与された順番に使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
				rideID, user.ID, coupon.Code,
			); err != nil {
				writeError(w, r, http.StatusInternalServerError, err)
				return
			}
		}
	}

	ride := Ride{}
	if err := tx.GetContext(ctx, &ride, "SELECT * FROM rides WHERE id = ?", rideID); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, &ride, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	func() {
		matchingRidesLock.Lock()
		defer matchingRidesLock.Unlock()

		matchingRides = append(matchingRides, &ride)
	}()
	rideCache.Store(rideID, &ride)
	rideStatusesCache.Store(rideID, &RideStatus{
		RideID: rideID,
		Status: "MATCHING",
	})
	UserPublish(ride.UserID, &RideEvent{
		status:    "MATCHING",
		updatedAt: now,
		ride:      &ride,
	})

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, r, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	discounted, err := calculateDiscountedFare(ctx, tx, user.ID, nil, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

var paymentTokenCache = isucache.NewAtomicMap[string, *PaymentToken]("paymentTokenCache")

func initPaymentTokenCache() error {
	paymentTokens := []PaymentToken{}
	if err := db.Select(&paymentTokens, "SELECT * FROM payment_tokens"); err != nil {
		return err
	}

	for _, paymentToken := range paymentTokens {
		paymentTokenCache.Store(paymentToken.UserID, &paymentToken)
	}

	return nil
}

var rideCache = isucache.NewAtomicMap[string, *Ride]("rideCache")

func initRideCache() error {
	rides := []Ride{}
	if err := db.Select(&rides, "SELECT * FROM rides"); err != nil {
		return err
	}

	for _, ride := range rides {
		rideCache.Store(ride.ID, &ride)
	}

	return nil
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	now := time.Now()

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, r, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	var ride *Ride
	exists := false
	rideCache.Update(rideID, func(v *Ride) (*Ride, bool) {
		if v == nil {
			return nil, false
		}

		ride = v
		exists = true
		ride.Evaluation = &req.Evaluation
		ride.UpdatedAt = now
		return v, true
	})
	if !exists {
		writeError(w, r, http.StatusNotFound, errors.New("ride not found"))
		return
	}
	status, err := getLatestRideStatus(ctx, db, ride.ID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if status == "COMPLETED" {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("already completed"))
		return
	}
	if status != "ARRIVED" {
		writeError(w, r, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE rides SET evaluation = ?, sales = ?, updated_at = ? WHERE id = ?`,
		req.Evaluation, initialFare+farePerDistance*calculateDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude), now, rideID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	if count, err := result.RowsAffected(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	} else if count == 0 {
		writeError(w, r, http.StatusNotFound, errors.New("ride not found"))
		return
	}

	if err := updateChairStatusToBadger(ride.ChairID.String, &chairStatus{
		status: chairStatusCompleted,
		rideID: rideID,
	}); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := updateUserStatusToBadger(ride.UserID, false); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	paymentToken, exists := paymentTokenCache.Load(ride.UserID)
	if !exists {
		writeError(w, r, http.StatusBadRequest, errors.New("payment token not registered"))
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	paymentGatewayRequest := &paymentGatewayPostPaymentRequest{
		Amount: fare,
	}

	if err := requestPaymentGatewayPostPayment(ctx, paymentGatewayURL, paymentToken.Token, paymentGatewayRequest); err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, r, http.StatusBadGateway, err)
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	rideStatusesCache.Store(rideID, &RideStatus{
		RideID: rideID,
		Status: "COMPLETED",
	})

	ChairPublish(ride.ChairID.String, &RideEvent{
		status:     "COMPLETED",
		evaluation: req.Evaluation,
		updatedAt:  now,
		ride:       ride,
	})
	UserPublish(ride.UserID, &RideEvent{
		status:     "COMPLETED",
		evaluation: req.Evaluation,
		updatedAt:  now,
		ride:       ride,
	})

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: now.UnixMilli(),
	})
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, r, http.StatusInternalServerError, errors.New("expected http.ResponseWriter to be an http.Flusher"))
	}

	ctx := r.Context()
	user := ctx.Value("user").(*User)

	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC LIMIT 1`, user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
				RetryAfterMs: 100,
			})
			return
		}
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFareDB(ctx, db, user.ID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	response := &appGetNotificationResponseData{
		RideID:                ride.ID,
		PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
		DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
		Fare:                  fare,
		CreatedAt:             ride.CreatedAt.UnixMilli(),
		UpdateAt:              ride.UpdatedAt.UnixMilli(),
	}

	response.Status, err = getLatestRideStatus(ctx, db, response.RideID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var stats appGetNotificationChairStats
	if ride.ChairID.Valid {
		chair := &Chair{}
		if err := db.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		stats, err = getChairStats(ctx, db, chair.ID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		evaluationAve := 0.0
		if stats.TotalRidesCount > 0 {
			evaluationAve = float64(stats.TotalEvaluation) / float64(stats.TotalRidesCount)
		}

		response.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: appGetNotificationResponseChairStats{
				TotalRidesCount:    stats.TotalRidesCount,
				TotalEvaluationAvg: evaluationAve,
			},
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	sb := &strings.Builder{}
	err = json.NewEncoder(sb).Encode(response)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to encode response1(%+v): %w", response.Chair, err))
		return
	}
	fmt.Fprintf(w, "data: %s\n", sb.String())
	flusher.Flush()

	ch := make(chan *RideEvent, 100)
	UserSubscribe(user.ID, ch)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			switch event.status {
			case "MATCHING":
				ride = event.ride

				fare, err := calculateDiscountedFareDB(ctx, db, user.ID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				response = &appGetNotificationResponseData{
					RideID:                ride.ID,
					PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
					DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
					Fare:                  fare,
					CreatedAt:             ride.CreatedAt.UnixMilli(),
					UpdateAt:              ride.UpdatedAt.UnixMilli(),
				}

				response.Status = event.status
			case "ENROUTE", "PICKUP", "CARRYING", "ARRIVED":
				response.Status = event.status
			case "MATCHED":
				chair := event.chair
				stats, err = getChairStats(ctx, db, chair.ID)
				if err != nil {
					writeError(w, r, http.StatusInternalServerError, err)
					return
				}

				evaluationAve := 0.0
				if stats.TotalRidesCount > 0 {
					evaluationAve = float64(stats.TotalEvaluation) / float64(stats.TotalRidesCount)
				}

				response.Chair = &appGetNotificationResponseChair{
					ID:    chair.ID,
					Name:  chair.Name,
					Model: chair.Model,
					Stats: appGetNotificationResponseChairStats{
						TotalRidesCount:    stats.TotalRidesCount,
						TotalEvaluationAvg: evaluationAve,
					},
				}
			case "COMPLETED":
				response.Status = event.status
				stats.TotalRidesCount++
				stats.TotalEvaluation += event.evaluation

				response.Chair.Stats = appGetNotificationResponseChairStats{
					TotalRidesCount:    stats.TotalRidesCount,
					TotalEvaluationAvg: float64(stats.TotalEvaluation) / float64(stats.TotalRidesCount),
				}
				response.UpdateAt = event.updatedAt.UnixMilli()
			}

			sb := &strings.Builder{}
			err = json.NewEncoder(sb).Encode(response)
			if err != nil {
				writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to encode response2(%+v): %w", response.Chair, err))
				return
			}
			fmt.Fprintf(w, "data: %s\n", sb.String())
			flusher.Flush()

			if response.Status == "COMPLETED" {
				return
			}
		}
	}
}

type appGetNotificationChairStats struct {
	TotalRidesCount int `json:"total_rides_count"`
	TotalEvaluation int `json:"total_evaluation_avg"`
}

func getChairStats(ctx context.Context, tx *sqlx.DB, chairID string) (appGetNotificationChairStats, error) {
	stats := appGetNotificationChairStats{}

	// Fetch all rides for the given chairID
	rides := []Ride{}
	err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC`,
		chairID,
	)
	if err != nil {
		return stats, err
	}

	if len(rides) == 0 {
		return stats, nil
	}

	// Collect all ride IDs
	rideIDs := make([]string, len(rides))
	for i, ride := range rides {
		rideIDs[i] = ride.ID
	}

	totalRideCount := 0
	totalEvaluation := 0

	for _, ride := range rides {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil || status != "COMPLETED" {
			continue
		}

		if ride.Evaluation != nil {
			totalRideCount++
			totalEvaluation += *ride.Evaluation
		}
	}

	stats.TotalRidesCount = totalRideCount
	stats.TotalEvaluation = totalEvaluation

	return stats, nil
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

var activeChairsCache *sc.Cache[string, []Chair]

func init() {
	var err error
	activeChairsCache, err = isucache.New("activeChairsCache", func(ctx context.Context, key string) ([]Chair, error) {
		chairs := []Chair{}
		if err := db.SelectContext(ctx, &chairs, `SELECT * FROM chairs WHERE is_active = TRUE`); err != nil {
			return nil, err
		}
		return chairs, nil
	}, 0, 300*time.Millisecond)
	if err != nil {
		panic(err)
	}
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, r, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	coordinate := Coordinate{Latitude: lat, Longitude: lon}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// Fetch all active chairs
	chairs, err := activeChairsCache.Get(ctx, "activeChairs")
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
			Chairs:      []appGetNearbyChairsResponseChair{},
			RetrievedAt: time.Now().UnixMilli(),
		})
		return
	}

	// Collect all chair IDs
	chairIDs := make([]string, len(chairs))
	for i, chair := range chairs {
		chairIDs[i] = chair.ID
	}

	chairLocationMap, err := getChairLocationsFromBadger(chairIDs)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	nearbyChairs := []appGetNearbyChairsResponseChair{}
	for _, chair := range chairs {
		// Check rides for this chair
		if ride, exists := latestRideCache.Load(chair.ID); exists {
			// 過去にライドが存在し、かつ、それが完了していない場合はスキップ
			status, exists := rideStatusesCache.Load(ride.ID)
			if !exists {
				writeError(w, r, http.StatusInternalServerError, fmt.Errorf("status not found for ride ID: %s", ride.ID))
				return
			}
			if status.Status != "COMPLETED" {
				continue
			}
		}

		// Get the latest ChairLocation
		chairLocation, exists := chairLocationMap[chair.ID]
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}
		if !exists {
			continue
		}

		if calculateDistance(coordinate.Latitude, coordinate.Longitude, chairLocation.LastLatitude, chairLocation.LastLongitude) <= distance {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
				ID:    chair.ID,
				Name:  chair.Name,
				Model: chair.Model,
				CurrentCoordinate: Coordinate{
					Latitude:  chairLocation.LastLatitude,
					Longitude: chairLocation.LastLongitude,
				},
			})
		}
	}

	retrievedAt := time.Now()

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt.UnixMilli(),
	})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}

func calculateDiscountedFare(ctx context.Context, tx *sqlx.Tx, userID string, ride *Ride, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if ride != nil {
		destLatitude = ride.DestinationLatitude
		destLongitude = ride.DestinationLongitude
		pickupLatitude = ride.PickupLatitude
		pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		} else {
			discount = coupon.Discount
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}

func calculateDiscountedFareDB(ctx context.Context, tx *sqlx.DB, userID string, ride *Ride, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if ride != nil {
		destLatitude = ride.DestinationLatitude
		destLongitude = ride.DestinationLongitude
		pickupLatitude = ride.PickupLatitude
		pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		} else {
			discount = coupon.Discount
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}
