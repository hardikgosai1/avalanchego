// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//nolint:gosec // Weak random number generator is acceptable for benchmark testing
package blockdb

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/utils/logging"
)

// Helper to generate a random block of realistic size and content
func generateRandomBlock(i int) []byte {
	blockSizeDistribution := []struct {
		size      int
		frequency float64
	}{
		{1024, 0.02},       // 1KB: 2% - Empty blocks (no txs beyond coinbase)
		{25 * 1024, 0.15},  // 25KB: 15% - Light-load blocks with few txs
		{80 * 1024, 0.60},  // 80KB: 60% - Most blocks cluster around the 80 KB mark
		{125 * 1024, 0.20}, // 125KB: 20% - Near gas-limit blocks
		{200 * 1024, 0.03}, // > 150KB: 3% - Rare, deeply-filled blocks
	}

	rng := rand.New(rand.NewSource(int64(i + 42)))
	r := rng.Float64()
	cumulativeFreq := 0.0
	selectedSize := blockSizeDistribution[0].size
	for _, sizeInfo := range blockSizeDistribution {
		cumulativeFreq += sizeInfo.frequency
		if r <= cumulativeFreq {
			selectedSize = sizeInfo.size
			break
		}
	}
	blockData := make([]byte, selectedSize)
	copy(blockData, fmt.Sprintf("block-data-for-height-%d", i+1))
	for j := len(fmt.Sprintf("block-data-for-height-%d", i+1)); j < len(blockData); j++ {
		blockData[j] = byte(rng.Intn(256))
	}
	return blockData
}

// Helper to fill a database with a given number of blocks with random sizes
// Returns a slice of the block data written, indexed by height-1
func fillDatabaseWithRandomBlocks(b *testing.B, db *Database, blockCount int) [][]byte {
	blocks := make([][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		blockData := generateRandomBlock(i)
		if err := db.WriteBlock(uint64(i+1), blockData, 0); err != nil {
			require.NoError(b, err, "Failed to write random block %d", i+1)
		}
		blocks[i] = blockData
	}
	return blocks
}

// Helper to create a benchmark database with temp dirs and cleanup
func createBenchDatabase(b *testing.B, syncToDisk bool) (db *Database, cleanup func()) {
	tempDir := b.TempDir()

	config := DefaultConfig().
		WithDir(tempDir).
		WithMinimumHeight(1).
		WithSyncToDisk(syncToDisk)

	db, err := New(config, logging.NoLog{})
	require.NoError(b, err, "Failed to create database")
	cleanup = func() {
		db.Close()
		// Directory cleanup is handled automatically by b.TempDir()
	}
	return db, cleanup
}

// Helper to fill a database with a given number of blocks of fixed size
// Returns a slice of the block data written, indexed by height-1
func fillDatabaseWithFixedBlocks(b *testing.B, db *Database, blockCount, blockSize int) [][]byte {
	blocks := make([][]byte, blockCount)
	for i := 0; i < blockCount; i++ {
		// Create fixed-size block with height information
		blockData := make([]byte, blockSize)
		heightStr := fmt.Sprintf("block-height-%d-", i+1)
		if len(heightStr) <= blockSize {
			copy(blockData, heightStr)
		}
		// Fill the rest with random data
		for j := len(heightStr); j < blockSize; j++ {
			blockData[j] = byte((i + j) % 256)
		}

		if err := db.WriteBlock(uint64(i+1), blockData, 0); err != nil {
			require.NoError(b, err, "Failed to write fixed block %d", i+1)
		}
		blocks[i] = blockData
	}
	return blocks
}

// BenchmarkReadBlock runs read related benchmarks on a prefilled database.
func BenchmarkReadBlock(b *testing.B) {
	const blockCount = 1000

	db, cleanup := createBenchDatabase(b, false)
	defer cleanup()
	blocks := fillDatabaseWithRandomBlocks(b, db, blockCount)

	b.Run("RandomRead", func(b *testing.B) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for i := 0; i < b.N; i++ {
			height := uint64(rng.Intn(blockCount) + 1)
			data, err := db.ReadBlock(height)
			require.NoError(b, err, "Failed to read block %d", height)
			expected := blocks[height-1]
			require.Equal(b, len(expected), len(data), "Read block %d has wrong size", height)
			require.Equal(b, string(expected), string(data), "Read block %d data mismatch", height)
		}
	})

	b.Run("RandomReadParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				height := uint64(rng.Intn(blockCount) + 1)
				data, err := db.ReadBlock(height)
				require.NoError(b, err, "Failed to read block %d", height)
				expected := blocks[height-1]
				require.Equal(b, len(expected), len(data), "Read block %d has wrong size", height)
				require.Equal(b, string(expected), string(data), "Read block %d data mismatch", height)
			}
		})
	})
}

// BenchmarkReadBlockFixedSize runs read related benchmarks on a prefilled database with fixed-size blocks.
func BenchmarkReadBlockFixedSize(b *testing.B) {
	const blockCount = 1000
	const blockSize = 80 * 1024 // 80KB per block

	db, cleanup := createBenchDatabase(b, false)
	defer cleanup()
	blocks := fillDatabaseWithFixedBlocks(b, db, blockCount, blockSize)

	b.Run("RandomReadParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(rand.Int63()))
			for pb.Next() {
				height := uint64(rng.Intn(blockCount) + 1)
				data, err := db.ReadBlock(height)
				require.NoError(b, err, "Failed to read block %d", height)
				expected := blocks[height-1]
				require.Equal(b, len(expected), len(data), "Read block %d has wrong size", height)
				require.Equal(b, string(expected), string(data), "Read block %d data mismatch", height)
			}
		})
	})
}

// BenchmarkWriteBlock runs write related benchmarks on a new database, writing random blocks.
func BenchmarkWriteBlock(b *testing.B) {
	const maxBlocks = 10_000

	// Pre-generate 10,000 random blocks before all tests
	blocks := make([][]byte, maxBlocks)
	for i := range blocks {
		blocks[i] = generateRandomBlock(i + 1)
	}

	b.Run("SequentialWrite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, cleanup := createBenchDatabase(b, false)
			b.StartTimer()
			start := time.Now()
			for h := 1; h <= maxBlocks; h++ {
				blockData := blocks[h-1]
				if err := db.WriteBlock(uint64(h), blockData, 0); err != nil {
					require.NoError(b, err, "Failed to write block %d", h)
				}
			}
			elapsed := time.Since(start)
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(maxBlocks), "ns/block")
			b.StopTimer()
			cleanup()
		}
	})

	b.Run("ParallelWrite", func(b *testing.B) {
		runParallelBlockWrite(b, blocks, false)
	})

	b.Run("ParallelWriteWithSyncToDisk", func(b *testing.B) {
		runParallelBlockWrite(b, blocks, true)
	})

	b.Run("ParallelWriteFixed20KB", func(b *testing.B) {
		const blockSize = 20 * 1024 // 20KB
		blocks := make([][]byte, maxBlocks)
		for i := 0; i < maxBlocks; i++ {
			// Create fixed-size block with height information
			blockData := make([]byte, blockSize)
			heightStr := fmt.Sprintf("block-height-%d-", i+1)
			if len(heightStr) <= blockSize {
				copy(blockData, heightStr)
			}
			// Fill the rest with random data
			for j := len(heightStr); j < blockSize; j++ {
				blockData[j] = byte((i + j) % 256)
			}
			blocks[i] = blockData
		}
		runParallelBlockWrite(b, blocks, false)
	})
}

// Helper for parallel block writing
func runParallelBlockWrite(b *testing.B, blocks [][]byte, syncToDisk bool) {
	numBlocks := len(blocks)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, cleanup := createBenchDatabase(b, syncToDisk)
		wg := sync.WaitGroup{}
		numGoroutines := runtime.NumCPU()
		wg.Add(numGoroutines)
		var height atomic.Uint64
		b.StartTimer()
		start := time.Now()
		for g := 0; g < numGoroutines; g++ {
			go func() {
				defer wg.Done()
				for {
					h := height.Add(1)
					if int(h) > numBlocks {
						break
					}
					blockData := blocks[h-1]
					_ = db.WriteBlock(h, blockData, 0) // Ignore errors for benchmarking
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(numBlocks), "ns/block")
		b.StopTimer()
		cleanup()
	}
}
