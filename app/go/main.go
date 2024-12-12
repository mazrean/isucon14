package main

import (
	crand "crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/goccy/go-json"

	"github.com/dgraph-io/badger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	isutools "github.com/mazrean/isucon-go-tools/v2"
	isudb "github.com/mazrean/isucon-go-tools/v2/db"
	isuhttp "github.com/mazrean/isucon-go-tools/v2/http"
	isupool "github.com/mazrean/isucon-go-tools/v2/pool"
	isuqueue "github.com/mazrean/isucon-go-tools/v2/queue"
)

var db *sqlx.DB
var paymentGatewayURL string = "http://43.207.87.29:12345"

func main() {
	mux := setup()
	slog.Info("Listening on :8080")

	err := os.MkdirAll(badgerDir, 0755)
	if err != nil {
		panic(fmt.Sprintf("failed to create badger directory: %v", err))
	}
	badgerDB, err = badger.Open(badger.DefaultOptions(badgerDir))
	if err != nil {
		panic(fmt.Sprintf("failed to open badger: %v", err))
	}
	defer badgerDB.Close()
	defer func() {
		badgerDB.Close()
	}()

	if err := initEmptyChairs(); err != nil {
		panic(err)
	}

	if err := initRideStatusesCache(); err != nil {
		panic(err)
	}

	if err := initPaymentTokenCache(); err != nil {
		panic(err)
	}

	if err := initRideCache(); err != nil {
		panic(err)
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
	isuqueue.AllReset()
	defer isutools.AfterInitialize()

	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}

	paymentGatewayURL = req.PaymentServer

	if err := initBadger(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	initEventBus()

	if err := initEmptyChairs(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := initRideStatusesCache(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := initPaymentTokenCache(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err := initRideCache(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	benchStartedAt = time.Now()

	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

var bufPool = isupool.NewSlice("buf", func() *[]byte {
	buf := make([]byte, 128)
	return &buf
})

func (c *Coordinate) bindJSON(r *http.Request) error {
	buf := bufPool.Get()
	defer bufPool.Put(buf)

	if _, err := r.Body.Read(*buf); err != nil {
		return err
	}

	str := unsafe.String(&(*buf)[0], len(*buf))
	str = strings.TrimPrefix(str, "{")
	str = strings.TrimSuffix(str, "}")
	str = strings.TrimSpace(str)
	left, right, found := strings.Cut(str, ",")
	if found {
		var latStr, lonStr string
		if strings.HasPrefix(left, `"latitude":`) && strings.HasPrefix(right, `"longitude":`) {
			latStr = left
			lonStr = right
		} else if strings.HasPrefix(left, `"longitude":`) && strings.HasPrefix(right, `"latitude":`) {
			latStr = right
			lonStr = left
		}

		if latStr != "" && lonStr != "" {
			lat, latErr := strconv.Atoi(strings.TrimPrefix(left, `"latitude":`))
			lon, lonErr := strconv.Atoi(strings.TrimPrefix(right, `"longitude":`))
			if latErr == nil && lonErr != nil {
				c.Latitude = lat
				c.Longitude = lon
				return nil
			}
		}
	}

	if err := json.Unmarshal([]byte(sb.String()), c); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}

	return nil
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
