package main

import (
	"database/sql"
	"errors"
	"net/http"
	"time"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. 椅子未割当のrideを全取得
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
	// このSQLは、前例同様、全ての関連rideがcompleted状態の椅子か関連rideが無い椅子を抽出する想定。
	queryChairs := `
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
	if err := db.SelectContext(ctx, &chairs, queryChairs); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 空いている椅子が無い
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// マンハッタン距離計算関数
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

	// 3. ridesごとに近い椅子を探すが、rideの発生から10秒以内は"近距離"の椅子を優先。
	//    10秒以内:
	//       - 近距離の範囲内(例えば500以内)に入る椅子のみ対象にし、その中から最も近い椅子を選択
	//       - 見つからなければ今回割り当てをしない（次回500ms後に再試行する）
	//    10秒経過後:
	//       - 妥協して全椅子から最も近いものを割り当てる。

	distanceThreshold := 500 // 適当な近接の閾値
	now := time.Now()

	assignments := make([]struct {
		chairID string
		rideID  string
		userID  string
	}, 0, len(rides))

	// availableChairsをコピーして使用済みを除くために管理
	availableChairs := chairs

	for _, ride := range rides {
		waitDuration := now.Sub(ride.CreatedAt)
		needCloseMatch := waitDuration < 10*time.Second

		// 対象となる椅子候補をフィルタ
		candidates := availableChairs
		if needCloseMatch {
			// 近距離の椅子のみ対象
			filtered := candidates[:0]
			for _, ch := range candidates {
				location, ok := chairLocationCache.Load(ch.ID)
				if !ok {
					continue
				}

				dist := manhattanDistance(ride.DestinationLatitude, ride.DestinationLongitude, location.LastLatitude, location.LastLongitude)
				if dist <= distanceThreshold {
					filtered = append(filtered, ch)
				}
			}
			candidates = filtered
		}

		// 候補がなければ、近距離を強要するなら今回は割り当てしないでスキップ
		// 次回呼ばれた時（500ms後）に再度試行
		if len(candidates) == 0 && needCloseMatch {
			continue
		}

		if len(candidates) == 0 && !needCloseMatch {
			// 10秒過ぎたけど候補なし → 全部使い果たした?
			// ありうるが、その場合割り当てできずに終了(またはcontinue)。
			// 次回再試行でも同じになる。ここではスキップ。
			continue
		}

		// 候補から最も近い椅子を探す
		bestIdx := -1
		bestDist := int(^uint(0) >> 1)
		for i, ch := range candidates {
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

		// 最適椅子が見つかったら assignments に追加
		if bestIdx >= 0 {
			chosenChair := candidates[bestIdx]

			assignments = append(assignments, struct {
				chairID string
				rideID  string
				userID  string
			}{
				chairID: chosenChair.ID,
				rideID:  ride.ID,
			})

			// chosenChairをavailableChairsから除去
			for i := range availableChairs {
				if availableChairs[i].ID == chosenChair.ID {
					availableChairs[i] = availableChairs[len(availableChairs)-1]
					availableChairs = availableChairs[:len(availableChairs)-1]
					break
				}
			}
		}
	}

	// 割り当てがなかった場合
	if len(assignments) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 4. トランザクションで割り当てを一括更新
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

		notificationResponseCache.Forget(a.userID)
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
