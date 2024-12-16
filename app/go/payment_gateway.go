package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/goccy/go-json"
	"github.com/oklog/ulid/v2"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

type paymentGatewayGetPaymentsResponseOne struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

func requestPaymentGatewayPostPayment(ctx context.Context, paymentGatewayURL string, token string, param *paymentGatewayPostPaymentRequest) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}

	idempotencyKey := ulid.Make().String()

	// 失敗したらとりあえずリトライ
	// FIXME: 社内決済マイクロサービスのインフラに異常が発生していて、同時にたくさんリクエストすると変なことになる可能性あり
	retry := 0
	for {
		err := func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", idempotencyKey)

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to request payment gateway: %w", err)
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusNoContent {
				return fmt.Errorf("unexpected status code: %d", res.StatusCode)
			}
			return nil
		}()
		if err != nil {
			if retry < 5 {
				retry++
				continue
			} else {
				slog.Error("failed to request payment gateway",
					slog.String("error", err.Error()),
				)
				return err
			}
		}
		break
	}

	return nil
}
