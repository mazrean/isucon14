package main

import (
	"database/sql"
	"errors"
	"net/http"
)

var chairModelSpeedCache = map[string]int{
	"AeroSeat":        3,
	"Aurora Glow":     7,
	"BalancePro":      3,
	"ComfortBasic":    2,
	"EasySit":         2,
	"ErgoFlex":        3,
	"Infinity Seat":   5,
	"Legacy Chair":    7,
	"LiteLine":        2,
	"LuxeThrone":      5,
	"Phoenix Ultra":   7,
	"ShadowEdition":   7,
	"SitEase":         2,
	"StyleSit":        3,
	"Titanium Line":   5,
	"ZenComfort":      5,
	"アルティマシート X":      5,
	"インフィニティ GEAR V":  7,
	"インペリアルクラフト LUXE": 5,
	"ヴァーチェア SUPREME":  7,
	"エアシェル ライト":       2,
	"エアフロー EZ":        3,
	"エコシート リジェネレイト":   7,
	"エルゴクレスト II":      3,
	"オブシディアン PRIME":   7,
	"クエストチェア Lite":    3,
	"ゲーミングシート NEXUS":  3,
	"シェルシート ハイブリッド":   3,
	"シャドウバースト M":      5,
	"ステルスシート ROGUE":   5,
	"ストリームギア S1":      3,
	"スピンフレーム 01":      2,
	"スリムライン GX":       5,
	"ゼノバース ALPHA":     7,
	"ゼンバランス EX":       5,
	"タイタンフレーム ULTRA":  7,
	"チェアエース S":        2,
	"ナイトシート ブラックエディション": 7,
	"フォームライン RX":        3,
	"フューチャーステップ VISION": 7,
	"フューチャーチェア CORE":    5,
	"プレイスタイル Z":         3,
	"フレックスコンフォート PRO":   3,
	"プレミアムエアチェア ZETA":   5,
	"プロゲーマーエッジ X1":      5,
	"ベーシックスツール プラス":     2,
	"モーションチェア RISE":     5,
	"リカーブチェア スマート":      3,
	"リラックスシート NEO":      2,
	"リラックス座":            2,
	"ルミナスエアクラウン":        7,
	"匠座 PRO LIMITED":    7,
	"匠座（たくみざ）プレミアム":     7,
	"雅楽座":        5,
	"風雅（ふうが）チェア": 3,
}

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
		writeError(w, http.StatusInternalServerError, err)
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
		bestDist := float64(^uint(0) >> 1) // 最大int値を初期値とする
		for i, ch := range availableChairs {
			location, ok := chairLocationCache.Load(ch.ID)
			if !ok {
				continue
			}

			length := manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, location.LastLatitude, location.LastLongitude) + manhattanDistance(ride.DestinationLatitude, ride.DestinationLongitude, ride.PickupLatitude, ride.PickupLongitude)
			dist := float64(length)*0.1 + float64(length)/float64(chairModelSpeedCache[ch.Model])
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
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 4. トランザクションで一括更新
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
