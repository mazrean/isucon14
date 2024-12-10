// hungarian_test.go
package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// generateCostMatrix は指定された行数と列数でランダムなコスト行列を生成します。
// コストは0から100の範囲のfloat64値です。
func generateCostMatrix(rows, cols int) [][]float64 {
	rand.Seed(time.Now().UnixNano())
	matrix := make([][]float64, rows)
	for i := 0; i < rows; i++ {
		matrix[i] = make([]float64, cols)
		for j := 0; j < cols; j++ {
			matrix[i][j] = rand.Float64() * 100
		}
	}
	return matrix
}

// BenchmarkHungarianAlgorithm はハンガリアン・アルゴリズムのベンチマークを行います。
func BenchmarkHungarianAlgorithm(b *testing.B) {
	// ベンチマークする行列のサイズ
	sizes := []struct {
		rows int
		cols int
	}{
		{10, 10},
		{50, 50},
		{100, 100},
		{200, 200},
		{500, 500},
	}

	for _, size := range sizes {
		b.Run("size", func(b *testing.B) {
			sizeName := fmt.Sprintf("%dx%d", size.rows, size.cols)
			hb := size
			b.Run(sizeName, func(b *testing.B) {
				// ベンチマーク対象の行列を生成
				matrix := generateCostMatrix(hb.rows, hb.cols)
				// タイマーをリセットして行列生成のコストを除外
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = HungarianAlgorithm(matrix)
				}
			})
		})
	}
}
