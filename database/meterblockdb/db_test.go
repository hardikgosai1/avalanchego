// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package meterblockdb

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/x/blockdb"
)

func TestMeterBlockDBMetricsCollection(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockDB := blockdb.NewMock()

	db, err := New(reg, "blockdb", mockDB)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Create 100 fixed-size blocks (1KB each)
	const blockCount = 100
	const blockSize = 1024 // 1KB
	const headerSize = 64  // 64 bytes header, rest is body

	blocks := make([][]byte, blockCount)
	for i := range blockCount {
		// Create fixed-size block with just a prefix, rest is zeros
		blockData := make([]byte, blockSize)
		prefix := fmt.Sprintf("block-%d", i)
		copy(blockData, prefix)
		blocks[i] = blockData
	}

	// Write all blocks
	for i := range blockCount {
		err := db.WriteBlock(uint64(i), blocks[i], headerSize)
		require.NoError(t, err)
	}

	// Read from blocks 0 to 119 (including non-existent ones)
	const blocksToRead = 120
	for height := range blocksToRead {
		// ReadBlock
		_, err := db.ReadBlock(uint64(height))
		if err != nil {
			require.Equal(t, blockdb.ErrBlockNotFound, err)
		}

		// ReadHeader
		_, err = db.ReadHeader(uint64(height))
		if err != nil {
			require.Equal(t, blockdb.ErrBlockNotFound, err)
		}

		// ReadBody
		_, err = db.ReadBody(uint64(height))
		if err != nil {
			require.Equal(t, blockdb.ErrBlockNotFound, err)
		}

		// HasBlock
		exists, err := db.HasBlock(uint64(height))
		require.NoError(t, err)
		if height >= 0 && height < blockCount {
			require.True(t, exists)
		} else {
			require.False(t, exists)
		}
	}

	// Close the database
	err = db.Close()
	require.NoError(t, err)

	// Gather and validate metrics
	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metrics)

	// Extract metric values
	calls := extractMetricValues(metrics, "calls")
	duration := extractMetricValues(metrics, "duration")
	size := extractMetricValues(metrics, "size")

	// Validate calls
	require.Equal(t, float64(blockCount), calls["write_block"])
	require.Equal(t, float64(blocksToRead*3), calls["read_block"]+calls["read_header"]+calls["read_body"])
	require.Equal(t, float64(blocksToRead), calls["has_block"])
	require.Equal(t, float64(1), calls["close"])

	// Validate duration (just check they're positive)
	for method, value := range duration {
		require.Greater(t, value, float64(0), "duration for %s should be positive", method)
	}

	// Validate size
	require.Equal(t, float64(blockCount*blockSize), size["write_block"])
	require.Equal(t, float64(blockCount*blockSize), size["read_block"])
	require.Equal(t, float64(blockCount*headerSize), size["read_header"])
	require.Equal(t, float64(blockCount*(blockSize-headerSize)), size["read_body"])
}

// Helper function to extract metric values by method
func extractMetricValues(metrics []*dto.MetricFamily, metricName string) map[string]float64 {
	result := make(map[string]float64)
	// Add namespace prefix to metric name
	namespacedMetricName := "blockdb_" + metricName

	for _, metric := range metrics {
		if *metric.Name == namespacedMetricName {
			for _, m := range metric.Metric {
				method := ""
				for _, label := range m.Label {
					if *label.Name == "method" {
						method = *label.Value
						break
					}
				}
				switch metricName {
				case "calls":
					result[method] = *m.Counter.Value
				case "duration":
					result[method] = *m.Gauge.Value
				case "size":
					result[method] = *m.Counter.Value
				}
			}
			break
		}
	}
	return result
}
