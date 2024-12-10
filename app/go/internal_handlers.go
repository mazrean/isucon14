package main

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"golang.org/x/exp/slog"
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
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 2. 空いている椅子を一括取得
	//   ロジック: 全ての関連rideがcompleted状態か、関連rideがない椅子を抽出。
	//   下記クエリ例は前回例と同様に適当に定義しています。
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
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		// 空き椅子なし
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// マンハッタン距離計算用関数
	manhattanDistance := func(x1, y1, x2, y2 int) int {
		dx := x1 - x2
		if dx < 0 {
			dx = -dx
		}
		dy := y1 - y2
		if dy < 0 {
			dy = -dy
		}
		return dx + dy
	}

	// 3. メモリ上でマンハッタン距離が最短になる椅子を割り当てる
	// 注意: rides数とchairs数が大きい場合、ここはO(N*M)になる
	var assignments []struct {
		chairID string
		rideID  string
		userID  string
	}

	// chairsを可変なsliceとして扱えるようにする
	availableChairs := chairs

	for _, ride := range rides {
		if len(availableChairs) == 0 {
			break // 椅子がもう無い
		}

		// rideに対して最も近いchairを探す
		bestIdx := -1
		bestDist := int(^uint(0) >> 1) // 最大int値を初期値とする
		for i, ch := range availableChairs {
			location, ok := chairLocationCache.Load(ch.ID)
			if !ok {
				continue
			}

			dist := manhattanDistance(ride.DestinationLatitude, ride.DestinationLongitude, location.LastLatitude, location.LastLongitude)
			if dist < bestDist {
				bestDist = dist
				bestIdx = i
			}
		}

		// 最適な椅子が見つかったら割り当て
		if bestIdx >= 0 {
			assignments = append(assignments, struct {
				chairID string
				rideID  string
				userID  string
			}{
				chairID: availableChairs[bestIdx].ID,
				rideID:  ride.ID,
				userID:  ride.UserID,
			})

			// 使用済みの椅子をリストから除去(末尾とスワップして削除する)
			availableChairs[bestIdx] = availableChairs[len(availableChairs)-1]
			availableChairs = availableChairs[:len(availableChairs)-1]
		}
	}

	// 割当がなかった場合
	if len(assignments) == 0 {
		slog.Info("no matching",
			"rides", len(rides),
			"chairs", len(chairs),
		)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 4. トランザクションで一括更新
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	for _, a := range assignments {
		if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", a.chairID, time.Now(), a.rideID); err != nil {
			tx.Rollback()
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

		slog.Info("ride matched",
			"ride_id", a.rideID,
			"chair_id", a.chairID,
			"user_id", a.userID,
		)

		ChairPublish(a.chairID, &RideEvent{
			status:  "MATCHED",
			chairID: a.chairID,
			rideID:  a.rideID,
		})
		UserPublish(a.userID, &RideEvent{
			status:  "MATCHED",
			chairID: a.chairID,
			rideID:  a.rideID,
		})
	}

	if err := tx.Commit(); err != nil {
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
