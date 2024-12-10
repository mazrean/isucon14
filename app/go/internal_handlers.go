package main

import (
	"database/sql"
	"errors"
	"math"
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

// Infinity は無限大を表します。
var InfinityFloat = math.Inf(1)

// Hungarian はハンガリアン・アルゴリズムの構造体です。
type Hungarian struct {
	n      int
	matrix [][]float64
	labelX []float64
	labelY []float64
	xy     []int
	yx     []int
	S      []bool
	T      []bool
	Slack  []float64
	Slackx []int
	prev   []int
}

// NewHungarian は新しいHungarian構造体を初期化します。
func NewHungarian(costMatrix [][]float64) *Hungarian {
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
				// ダミー行・列のコストを0に設定
				squareMatrix[i][j] = 0.0
			}
		}
	}

	h := &Hungarian{
		n:      n,
		matrix: squareMatrix,
		labelX: make([]float64, n),
		labelY: make([]float64, n),
		xy:     make([]int, n),
		yx:     make([]int, n),
		S:      make([]bool, n),
		T:      make([]bool, n),
		Slack:  make([]float64, n),
		Slackx: make([]int, n),
		prev:   make([]int, n),
	}

	// 初期ラベル設定
	for x := 0; x < n; x++ {
		h.labelX[x] = h.minInRow(x)
	}
	for y := 0; y < n; y++ {
		h.labelY[y] = h.minInCol(y)
	}

	// 初期マッチングの作成
	for x := 0; x < n; x++ {
		for y := 0; y < n; y++ {
			if h.matrix[x][y] == h.labelX[x]+h.labelY[y] && h.yx[y] == -1 {
				h.xy[x] = y
				h.yx[y] = x
				break
			}
		}
	}

	// 初期化: -1 は未マッチを示す
	for i := range h.xy {
		if h.xy[i] == 0 && !h.isMatched(i) {
			h.xy[i] = -1
		}
	}

	return h
}

// minInRow は指定された行の最小値を返します。
func (h *Hungarian) minInRow(x int) float64 {
	min := InfinityFloat
	for y := 0; y < h.n; y++ {
		if h.matrix[x][y] < min {
			min = h.matrix[x][y]
		}
	}
	return min
}

// minInCol は指定された列の最小値を返します。
func (h *Hungarian) minInCol(y int) float64 {
	min := InfinityFloat
	for x := 0; x < h.n; x++ {
		if h.matrix[x][y] < min {
			min = h.matrix[x][y]
		}
	}
	return min
}

// isMatched は列yがマッチしているかを返します。
func (h *Hungarian) isMatched(y int) bool {
	return h.yx[y] != -1
}

// Execute はハンガリアン・アルゴリズムを実行します。
func (h *Hungarian) Execute() {
	for {
		// S, T, Slack, prev を初期化
		h.S = make([]bool, h.n)
		h.T = make([]bool, h.n)
		for j := 0; j < h.n; j++ {
			h.Slack[j] = InfinityFloat
		}
		for i := 0; i < h.n; i++ {
			h.prev[i] = -1
		}

		// 初期の未マッチの行を探す
		root := -1
		for x := 0; x < h.n; x++ {
			if h.xy[x] == -1 {
				root = x
				h.S[x] = true
				for y := 0; y < h.n; y++ {
					if h.labelX[x]+h.labelY[y]-h.matrix[x][y] < h.Slack[y] {
						h.Slack[y] = h.labelX[x] + h.labelY[y] - h.matrix[x][y]
						h.Slackx[y] = x
					}
				}
				break
			}
		}

		if root == -1 {
			break // 全てマッチしている
		}

		// BFS用のキュー
		queue := []int{root}
		found := false
		var x, y int

		for len(queue) > 0 && !found {
			x = queue[0]
			queue = queue[1:]

			for y = 0; y < h.n; y++ {
				if h.matrix[x][y] == h.labelX[x]+h.labelY[y] && !h.T[y] {
					if h.yx[y] == -1 {
						// 増加パスを見つけた
						found = true
						h.prev[x] = y
						break
					}
					h.T[y] = true
					queue = append(queue, h.yx[y])
					h.S[h.yx[y]] = true
					for j := 0; j < h.n; j++ {
						if h.labelX[h.yx[y]]+h.labelY[j]-h.matrix[h.yx[y]][j] < h.Slack[j] {
							h.Slack[j] = h.labelX[h.yx[y]] + h.labelY[j] - h.matrix[h.yx[y]][j]
							h.Slackx[j] = h.yx[y]
						}
					}
				}
			}
		}

		if found {
			// 増加パスをたどってマッチングを更新
			x, y = h.prev[x], y
			for {
				prevY := h.xy[x]
				h.xy[x] = y
				h.yx[y] = x
				if prevY == -1 {
					break
				}
				x = h.prev[h.yx[prevY]]
				y = prevY
			}
		} else {
			// ラベルを調整
			delta := InfinityFloat
			for j := 0; j < h.n; j++ {
				if !h.T[j] && h.Slack[j] < delta {
					delta = h.Slack[j]
				}
			}

			for i := 0; i < h.n; i++ {
				if h.S[i] {
					h.labelX[i] -= delta
				}
			}
			for j := 0; j < h.n; j++ {
				if h.T[j] {
					h.labelY[j] += delta
				} else {
					h.Slack[j] -= delta
				}
			}

			// 再探索
			for j := 0; j < h.n; j++ {
				if !h.T[j] && h.Slack[j] == 0 {
					if h.yx[j] == -1 {
						// 増加パスを見つけた
						x = h.Slackx[j]
						y = j
						found = true
						break
					} else {
						h.T[j] = true
						queue = append(queue, h.yx[j])
						h.S[h.yx[j]] = true
						for k := 0; k < h.n; k++ {
							if h.labelX[h.yx[j]]+h.labelY[k]-h.matrix[h.yx[j]][k] < h.Slack[k] {
								h.Slack[k] = h.labelX[h.yx[j]] + h.labelY[k] - h.matrix[h.yx[j]][k]
								h.Slackx[k] = h.yx[j]
							}
						}
					}
				}
			}

			if found {
				// 増加パスをたどってマッチングを更新
				x = h.Slackx[y]
				for {
					prevY := h.xy[x]
					h.xy[x] = y
					h.yx[y] = x
					if prevY == -1 {
						break
					}
					x = h.Slackx[prevY]
					y = prevY
				}
			}
		}
	}
}

// HungarianAlgorithm はハンガリアン・アルゴリズムを実装します。
// costMatrix は割り当てコストの行列（非正方行列も可）です。
// 戻り値は最小コストと割り当て結果のスライスです。
func HungarianAlgorithm(costMatrix [][]float64) (float64, []int) {
	matcher := NewHungarian(costMatrix)
	matcher.Execute()

	rows := len(costMatrix)
	cols := len(costMatrix[0])
	n := matcher.n

	totalCost := 0.0
	assignment := make([]int, rows)
	for i := 0; i < rows; i++ {
		assignment[i] = -1
	}

	for x := 0; x < n; x++ {
		y := matcher.xy[x]
		if x < rows && y < cols {
			assignment[x] = y
			totalCost += costMatrix[x][y]
		}
	}

	return totalCost, assignment
}
