package main

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	isutools "github.com/mazrean/isucon-go-tools/v2"
	isudb "github.com/mazrean/isucon-go-tools/v2/db"
	isuhttp "github.com/mazrean/isucon-go-tools/v2/http"
)

var db *sqlx.DB

func main() {
	mux := setup()
	slog.Info("Listening on :8080")

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
		panic(err)
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
		panic(err)
	}

	chairLatestLocationMap := make(map[string]Coordinate)
	for _, loc := range chairLatestLocations {
		chairLatestLocationMap[loc.ChairID] = Coordinate{
			Latitude:  loc.LastLatitude,
			Longitude: loc.LastLongitude,
		}
	}

	for _, loc := range chairLocations {
		chairLocationCache.Store(loc.ChairID, &chairLocation{
			TotalDistance:          loc.TotalDist,
			LastLatitude:           chairLatestLocationMap[loc.ChairID].Latitude,
			LastLongitude:          chairLatestLocationMap[loc.ChairID].Longitude,
			TotalDistanceUpdatedAt: loc.UpdatedAt,
		})
	}

	isuhttp.ListenAndServe(":8080", mux)
}

func setup() http.Handler {
	host := os.Getenv("ISUCON_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ISUCON_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("failed to convert DB port number from ISUCON_DB_PORT environment variable into int: %v", err))
	}
	user := os.Getenv("ISUCON_DB_USER")
	if user == "" {
		user = "isucon"
	}
	password := os.Getenv("ISUCON_DB_PASSWORD")
	if password == "" {
		password = "isucon"
	}
	dbname := os.Getenv("ISUCON_DB_NAME")
	if dbname == "" {
		dbname = "isuride"
	}

	dbConfig := mysql.NewConfig()
	dbConfig.User = user
	dbConfig.Passwd = password
	dbConfig.Addr = net.JoinHostPort(host, port)
	dbConfig.Net = "tcp"
	dbConfig.DBName = dbname
	dbConfig.ParseTime = true

	_db, err := isudb.DBMetricsSetup(sqlx.Connect)("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}
	db = _db

	mux := chi.NewRouter()
	mux.Use(middleware.Recoverer)
	mux.HandleFunc("POST /api/initialize", postInitialize)

	// app handlers
	{
		mux.HandleFunc("POST /api/app/users", appPostUsers)

		authedMux := mux.With(appAuthMiddleware)
		authedMux.HandleFunc("POST /api/app/payment-methods", appPostPaymentMethods)
		authedMux.HandleFunc("GET /api/app/rides", appGetRides)
		authedMux.HandleFunc("POST /api/app/rides", appPostRides)
		authedMux.HandleFunc("POST /api/app/rides/estimated-fare", appPostRidesEstimatedFare)
		authedMux.HandleFunc("POST /api/app/rides/{ride_id}/evaluation", appPostRideEvaluatation)
		authedMux.HandleFunc("GET /api/app/notification", appGetNotification)
		authedMux.HandleFunc("GET /api/app/nearby-chairs", appGetNearbyChairs)
	}

	// owner handlers
	{
		mux.HandleFunc("POST /api/owner/owners", ownerPostOwners)

		authedMux := mux.With(ownerAuthMiddleware)
		authedMux.HandleFunc("GET /api/owner/sales", ownerGetSales)
		authedMux.HandleFunc("GET /api/owner/chairs", ownerGetChairs)
	}

	// chair handlers
	{
		mux.HandleFunc("POST /api/chair/chairs", chairPostChairs)

		authedMux := mux.With(chairAuthMiddleware)
		authedMux.HandleFunc("POST /api/chair/activity", chairPostActivity)
		authedMux.HandleFunc("POST /api/chair/coordinate", chairPostCoordinate)
		authedMux.HandleFunc("GET /api/chair/notification", chairGetNotification)
		authedMux.HandleFunc("POST /api/chair/rides/{ride_id}/status", chairPostRideStatus)
	}

	// internal handlers
	{
		mux.HandleFunc("GET /api/internal/matching", internalGetMatching)
	}

	return mux
}

type postInitializeRequest struct {
	PaymentServer string `json:"payment_server"`
}

type postInitializeResponse struct {
	Language string `json:"language"`
}

func postInitialize(w http.ResponseWriter, r *http.Request) {
	isutools.BeforeInitialize()
	defer isutools.AfterInitialize()

	ctx := r.Context()
	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.PaymentServer); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var chairLocations []struct {
		ChairID   string    `db:"chair_id"`
		TotalDist int       `db:"total_distance"`
		UpdatedAt time.Time `db:"total_distance_updated_at"`
	}
	if err := db.SelectContext(ctx, &chairLocations, `SELECT chair_id,
		SUM(IFNULL(distance, 0)) AS total_distance,
		MAX(created_at)          AS total_distance_updated_at
	FROM (SELECT chair_id,
			created_at,
			ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
			ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
		FROM chair_locations) tmp
		GROUP BY chair_id`); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	var chairLatestLocations []struct {
		ChairID       string `db:"chair_id"`
		LastLatitude  int    `db:"latitude"`
		LastLongitude int    `db:"longitude"`
	}
	if err := db.SelectContext(ctx, &chairLatestLocations, `SELECT cl.chair_id,
		cl.latitude,
		cl.longitude
	FROM chair_locations cl
	JOIN (SELECT chair_id, MAX(created_at) AS created_at
		FROM chair_locations
		GROUP BY chair_id) cl2
	ON cl.chair_id = cl2.chair_id AND cl.created_at = cl2.created_at`); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	chairLatestLocationMap := make(map[string]Coordinate)
	for _, loc := range chairLatestLocations {
		chairLatestLocationMap[loc.ChairID] = Coordinate{
			Latitude:  loc.LastLatitude,
			Longitude: loc.LastLongitude,
		}
	}

	for _, loc := range chairLocations {
		chairLocationCache.Store(loc.ChairID, &chairLocation{
			TotalDistance:          loc.TotalDist,
			LastLatitude:           chairLatestLocationMap[loc.ChairID].Latitude,
			LastLongitude:          chairLatestLocationMap[loc.ChairID].Longitude,
			TotalDistanceUpdatedAt: loc.UpdatedAt,
		})
	}

	initEventBus()

	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

func bindJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	buf, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, r *http.Request, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	buf, marshalError := json.Marshal(map[string]string{"message": err.Error()})
	if marshalError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"marshaling error failed"}`))
		return
	}
	w.Write(buf)

	slog.Error("error response wrote",
		slog.String("path", r.URL.Path),
		slog.Int("status_code", statusCode),
		slog.String("error", err.Error()),
	)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}
