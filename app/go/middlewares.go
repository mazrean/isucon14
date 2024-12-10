package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	isucache "github.com/mazrean/isucon-go-tools/v2/cache"
	"github.com/motoki317/sc"
)

var accessTokenCache *sc.Cache[string, *User]

func init() {
	var err error
	accessTokenCache, err = isucache.New[string, *User](
		"userCache",
		func(ctx context.Context, key string) (*User, error) {
			user := &User{}
			err := db.GetContext(ctx, user, "SELECT * FROM users WHERE access_token = ?", key)
			if err != nil {
				return nil, err
			}
			return user, nil
		},
		5*time.Minute,  // freshFor
		10*time.Minute, // ttl
		sc.WithCleanupInterval(1*time.Minute),
	)
	if err != nil {
		// Handle cache initialization error appropriately
		panic(err)
	}
}

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("app_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, r, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		accessToken := c.Value

		user, err := accessTokenCache.Get(ctx, accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		ctx = context.WithValue(ctx, "user", user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var (
	ownerCache     *sc.Cache[string, *Owner]
	ownerCacheOnce sync.Once
)

func ownerAuthMiddleware(next http.Handler) http.Handler {
	ownerCacheOnce.Do(func() {
		var err error
		ownerCache, err = isucache.New("ownerCache", func(ctx context.Context, key string) (*Owner, error) {
			owner := &Owner{}
			if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE access_token = ?", key); err != nil {
				return nil, err
			}
			return owner, nil
		}, 5*time.Minute, 10*time.Minute, sc.WithMapBackend(1000), sc.EnableStrictCoalescing())
		if err != nil {
			log.Fatalf("failed to create owner cache: %v", err)
		}
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("owner_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, r, http.StatusUnauthorized, errors.New("owner_session cookie is required"))
			return
		}
		accessToken := c.Value

		owner, err := ownerCache.Get(ctx, accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		ctx = context.WithValue(ctx, "owner", owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func chairAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("chair_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, r, http.StatusUnauthorized, errors.New("chair_session cookie is required"))
			return
		}
		accessToken := c.Value
		chair := &Chair{}
		err = db.GetContext(ctx, chair, "SELECT * FROM chairs WHERE access_token = ?", accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, r, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		ctx = context.WithValue(ctx, "chair", chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
