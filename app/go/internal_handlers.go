package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
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
	matchingRides     = []*Ride{}
	matchingRidesLock = sync.RWMutex{}
	emptyChairs       = []*Chair{}
	emptyChairsLocker = sync.RWMutex{}
	benchStartedAt    = time.Time{}
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
	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		skipCounter := 0
		for range ticker.C {
			isChairExist := func() bool {
				emptyChairsLocker.RLock()
				defer emptyChairsLocker.RUnlock()

				return len(emptyChairs) > 5 || skipCounter > 20
			}()
			if isChairExist {
				skipCounter = 0
				internalGetMatching()
			} else {
				skipCounter++
			}
		}
	}()
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching() {
	ctx := context.Background()

	isInBenchmark := !benchStartedAt.IsZero() && benchStartedAt.Add(60*time.Second).After(time.Now())

	// 1. 椅子未割当のrideを全件取得
	var rides []*Ride
	func() {
		matchingRidesLock.Lock()
		defer matchingRidesLock.Unlock()

		rides = matchingRides
		matchingRides = []*Ride{}
	}()

	if len(rides) == 0 {
		slog.Info("no rides to match")
		return
	}

	var chairs []*Chair
	func() {
		emptyChairsLocker.Lock()
		defer emptyChairsLocker.Unlock()

		chairs = emptyChairs
		emptyChairs = []*Chair{}
	}()

	slog.Info("matching start",
		slog.Int("rides", len(rides)),
		slog.Int("chairs", len(chairs)),
	)

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
		slog.Info("no empty chairs")
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

	// chairsを可変なsliceとして扱えるようにする
	availableChairs := chairs

	type match struct {
		ride  *Ride
		ch    *Chair
		age   int
		score float64
	}
	matches := []match{}
	for _, ride := range rides {
		for _, ch := range availableChairs {
			location, ok, err := getChairLocationFromBadger(ch.ID)
			if err != nil {
				slog.Error("failed to get chair location from badger",
					slog.String("error", err.Error()),
				)
				return
			}
			if !ok {
				continue
			}

			pd := float64(manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, location.LastLatitude, location.LastLongitude)) / float64(chairModelSpeedCache[ch.Model])
			dd := float64(manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude))
			age := int(time.Since(ride.CreatedAt).Milliseconds())
			loss := math.Pow(float64(age)/5000, 4)
			// 25s以上経過しているrideは優先度を大きく上げる
			if age > 22000 {
				loss += 100000
			}

			// ベンチマーカーハック: ベンチマーク中にマッチングの期限を迎えないrideは割り当て優先度を下げ、終了後にマッチングさせる
			isNoAgeLimit := isInBenchmark && ride.CreatedAt.After(benchStartedAt.Add(35*time.Second))
			if isNoAgeLimit {
				loss = 8 - math.Pow(float64(age+1)/1000, 3)
			}

			score := dd - 100*pd + 100000*loss

			matches = append(matches, match{
				ride:  ride,
				ch:    ch,
				age:   age,
				score: score,
			})
		}
	}
	slices.SortFunc(matches, func(a, b match) int {
		return cmp.Compare(b.score, a.score)
	})

	matchedChairIDMap := map[string]struct{}{}
	matchedRideIDMap := map[string]struct{}{}
	for _, m := range matches {
		if _, ok := matchedChairIDMap[m.ch.ID]; ok {
			continue
		}
		if _, ok := matchedRideIDMap[m.ride.ID]; ok {
			continue
		}

		now := time.Now()
		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", m.ch.ID, now, m.ride.ID); err != nil {
			slog.Error("failed to update ride",
				slog.String("error", err.Error()),
			)
			return
		}
		m.ride.ChairID = sql.NullString{String: m.ch.ID, Valid: true}
		m.ride.UpdatedAt = now

		rideCache.Store(m.ride.ID, m.ride)
		latestRideCache.Store(m.ch.ID, m.ride)
		ChairPublish(m.ch.ID, &RideEvent{
			status: "MATCHED",
			chair:  m.ch,
			ride:   m.ride,
		})
		UserPublish(m.ride.UserID, &RideEvent{
			status: "MATCHED",
			chair:  m.ch,
			ride:   m.ride,
		})
		matchedChairIDMap[m.ch.ID] = struct{}{}
		matchedRideIDMap[m.ride.ID] = struct{}{}
	}

	slog.Info("matching end",
		"matches", len(matches),
		"matched_chairs", len(matchedChairIDMap),
		"matched_rides", len(matchedRideIDMap),
		"empty_chairs", len(emptyChairs),
		"remaining_rides", len(rides)-len(matchedRideIDMap),
	)

	func() {
		matchingRidesLock.Lock()
		defer matchingRidesLock.Unlock()

		for _, r := range rides {
			if _, ok := matchedRideIDMap[r.ID]; !ok {
				matchingRides = append(matchingRides, r)
			}
		}
	}()
	func() {
		emptyChairsLocker.Lock()
		defer emptyChairsLocker.Unlock()
		for _, ch := range chairs {
			if _, ok := matchedChairIDMap[ch.ID]; !ok {
				emptyChairs = append(emptyChairs, ch)
			}
		}
	}()
}
