package main

import (
	"database/sql"
	"errors"
	"net/http"
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

	table := make([][]float64, 0, len(rides))
	for _, ride := range rides {
		row := make([]float64, 0, len(availableChairs))
		for _, chair := range availableChairs {
			location, ok := chairLocationCache.Load(chair.ID)
			if !ok {
				continue
			}

			dist := float64(manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, location.LastLatitude, location.LastLongitude)+manhattanDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)) / float64(chairModelSpeedCache[chair.Model])
			row = append(row, dist)
		}
		table = append(table, row)
	}

	_, assignmentResults := HungarianAlgorithm(table)

	for i, ride := range rides {
		if assignmentResults[i] >= len(availableChairs) || assignmentResults[i] < 0 {
			continue
		}

		assignments = append(assignments, struct {
			chairID string
			rideID  string
			userID  string
		}{
			chairID: availableChairs[assignmentResults[i]].ID,
			rideID:  ride.ID,
			userID:  ride.UserID,
		})
	}

	// 割当がなかった場合
	if len(assignments) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	for _, a := range assignments {
		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ?, updated_at = ? WHERE id = ?", a.chairID, time.Now(), a.rideID); err != nil {
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

	w.WriteHeader(http.StatusNoContent)
}

const Infinity = float64(^uint(0) >> 1)

// HungarianAlgorithm はハンガリアン・アルゴリズムを実装します。
// costMatrix は割り当てコストの行列（非正方行列も可）です。
// 戻り値は最小コストと割り当て結果のペアです。
func HungarianAlgorithm(costMatrix [][]float64) (float64, []int) {
	rows := len(costMatrix)
	cols := len(costMatrix[0])

	// 行列を正方行列に拡張
	n := max(rows, cols)
	squareMatrix := make([][]float64, n)
	for i := 0; i < n; i++ {
		squareMatrix[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			if i < rows && j < cols {
				squareMatrix[i][j] = costMatrix[i][j]
			} else {
				// ダミー行・列のコストをintの最大値に設定
				squareMatrix[i][j] = Infinity
			}
		}
	}

	// 行の最小値を引く
	for i := 0; i < n; i++ {
		min := squareMatrix[i][0]
		for j := 1; j < n; j++ {
			if squareMatrix[i][j] < min {
				min = squareMatrix[i][j]
			}
		}
		for j := 0; j < n; j++ {
			squareMatrix[i][j] -= min
		}
	}

	// 列の最小値を引く
	for j := 0; j < n; j++ {
		min := squareMatrix[0][j]
		for i := 1; i < n; i++ {
			if squareMatrix[i][j] < min {
				min = squareMatrix[i][j]
			}
		}
		for i := 0; i < n; i++ {
			squareMatrix[i][j] -= min
		}
	}

	// マッチングのための配列
	mate := make([]int, n)
	for i := range mate {
		mate[i] = -1
	}

	// 初期マッチングの作成
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if squareMatrix[i][j] == 0 && mate[j] == -1 {
				mate[j] = i
				break
			}
		}
	}

	// カバー行とカバー列のフラグ
	coveredRows := make([]bool, n)
	coveredCols := make([]bool, n)

	for {
		// ゼロのカバーを行う
		coveredRows = make([]bool, n)
		coveredCols = make([]bool, n)

		// ステップ1: 独立ゼロを選び、カバー
		zeros := findIndependentZeros(squareMatrix, n)
		for _, zero := range zeros {
			coveredRows[zero[0]] = true
			coveredCols[zero[1]] = true
		}

		// カバーされた行と列の数をカウント
		coveredCount := 0
		for _, c := range coveredCols {
			if c {
				coveredCount++
			}
		}
		for _, r := range coveredRows {
			if r {
				coveredCount++
			}
		}

		// カバーされた数がn未満なら調整
		if coveredCount < n {
			// 最小未カバー値を見つける
			minUncovered := Infinity
			for i := 0; i < n; i++ {
				if !coveredRows[i] {
					for j := 0; j < n; j++ {
						if !coveredCols[j] && squareMatrix[i][j] < minUncovered {
							minUncovered = squareMatrix[i][j]
						}
					}
				}
			}

			// 最小値を調整
			for i := 0; i < n; i++ {
				for j := 0; j < n; j++ {
					if coveredRows[i] && coveredCols[j] {
						squareMatrix[i][j] += minUncovered
					} else if !coveredRows[i] && !coveredCols[j] {
						squareMatrix[i][j] -= minUncovered
					}
				}
			}

			// 再度マッチングを試みる
			for i := 0; i < n; i++ {
				for j := 0; j < n; j++ {
					if squareMatrix[i][j] == 0 && mate[j] == -1 {
						mate[j] = i
					}
				}
			}
		} else {
			break
		}
	}

	// 結果の割り当てと総コストを計算
	totalCost := 0.0
	assignment := make([]int, rows) // 元の行数に対応
	for i := range assignment {
		assignment[i] = -1
	}
	for i := 0; i < n; i++ {
		if mate[i] < rows && i < cols {
			assignment[mate[i]] = i
			totalCost += costMatrix[mate[i]][i]
		}
	}

	return totalCost, assignment
}

// findIndependentZeros は独立したゼロの位置を見つけます。
func findIndependentZeros(matrix [][]float64, n int) [][2]int {
	mate := make([]int, n)
	for i := range mate {
		mate[i] = -1
	}
	zeros := [][2]int{}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if matrix[i][j] == 0 && mate[j] == -1 {
				mate[j] = i
				zeros = append(zeros, [2]int{i, j})
				break
			}
		}
	}
	return zeros
}
