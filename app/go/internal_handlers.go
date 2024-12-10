package main

import (
	"database/sql"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"golang.org/x/exp/slog"
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

var (
	emptyChairs       = []*Chair{}
	emptyChairsLocker = sync.RWMutex{}
)

func initEmptyChairs() error {
	emptyChairsLocker.Lock()
	defer emptyChairsLocker.Unlock()

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
	if err := db.Select(&emptyChairs, query); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}

		return err
	}

	return nil
}

func init() {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range ticker.C {
			isChairExist := func() bool {
				emptyChairsLocker.RLock()
				defer emptyChairsLocker.RUnlock()

				return len(emptyChairs) > 5
			}()
			if isChairExist {
				internalGetMatching(httptest.NewRecorder(), &http.Request{})
			}
		}
	}()
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
		writeError(w, r, http.StatusInternalServerError, err)
		return
	}

	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var chairs []*Chair
	func() {
		emptyChairsLocker.Lock()
		defer emptyChairsLocker.Unlock()

		chairs = emptyChairs
		emptyChairs = []*Chair{}
	}()

	chairMap := map[string]*Chair{}
	for _, ch := range chairs {
		chairMap[ch.ID] = ch
	}

	chairs = chairs[:0]
	for _, ch := range chairMap {
		chairs = append(chairs, ch)
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

	slog.Info("matching start",
		"rides", len(rides),
		"chairs", len(chairs),
	)

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
		bestDist := math.MaxFloat64
		for i, ch := range availableChairs {
			location, ok := chairLocationCache.Load(ch.ID)
			if !ok {
				continue
			}

			dist := float64(manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, location.LastLatitude, location.LastLongitude)+manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)) / float64(chairModelSpeedCache[ch.Model])
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

	matchedChairIDMap := map[string]struct{}{}
	for _, a := range assignments {
		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", a.chairID, time.Now(), a.rideID); err != nil {
			writeError(w, r, http.StatusInternalServerError, err)
			return
		}

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
		matchedChairIDMap[a.chairID] = struct{}{}
	}

	for _, ch := range chairs {
		if _, ok := matchedChairIDMap[ch.ID]; !ok {
			func() {
				emptyChairsLocker.Lock()
				defer emptyChairsLocker.Unlock()

				emptyChairs = append(emptyChairs, ch)
			}()
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
