package main

import (
	"database/sql"
	"errors"
	"math/rand/v2"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. 椅子未割当のrideを全件取得
	var rides []Ride
	if err := db.SelectContext(ctx, &rides, `
        SELECT *
        FROM rides
        WHERE chair_id IS NULL
        ORDER BY created_at
    `); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 2. 空いている椅子を一括取得
	//    ロジック: 全ての関連rideが "completed"(count(chair_sent_at)=6) な椅子、もしくは関連rideが一切無い椅子を抽出
	//    chairsテーブルとrides、ride_statusesテーブルを結合・集約し、completedでないrideがない椅子を抽出する
	query := `
SELECT c.*
FROM chairs c
LEFT JOIN rides r ON r.chair_id = c.id
LEFT JOIN (
    SELECT ride_id, (COUNT(chair_sent_at) = 6) AS completed
    FROM ride_statuses
    GROUP BY ride_id
) rs ON rs.ride_id = r.id
WHERE c.is_active = TRUE
GROUP BY c.id
HAVING SUM(CASE WHEN rs.completed = 0 AND rs.completed IS NOT NULL THEN 1 ELSE 0 END) = 0
`
	var chairs []Chair
	if err := db.SelectContext(ctx, &chairs, query); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 空いている椅子が無い場合
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(chairs) == 0 {
		// 空き椅子なし
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// ランダムな順番に並べて割当
	rand.Shuffle(len(chairs), func(i, j int) {
		chairs[i], chairs[j] = chairs[j], chairs[i]
	})

	// 3. ridesとchairsをメモリ上でマッチング
	//    chairsが足りなくなったらそこで割り当て終了
	var assignments []struct {
		chairID string
		rideID  string
		userID  string
	}

	for i, ride := range rides {
		if i >= len(chairs) {
			break
		}
		assignments = append(assignments, struct {
			chairID string
			rideID  string
			userID  string
		}{
			chairID: chairs[i].ID,
			rideID:  ride.ID,
			userID:  ride.UserID,
		})
	}

	// 割当がなかった場合
	if len(assignments) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 4. トランザクションを用いて一括更新
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	for _, a := range assignments {
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", a.chairID, a.rideID); err != nil {
			tx.Rollback()
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	for _, a := range assignments {
		notificationResponseCache.Forget(a.userID)
	}

	w.WriteHeader(http.StatusNoContent)
}
