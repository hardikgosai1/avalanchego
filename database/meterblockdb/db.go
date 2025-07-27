// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package meterblockdb

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ava-labs/avalanchego/database"
)

const methodLabel = "method"

var (
	_ database.BlockDatabase = (*Database)(nil)

	methodLabels    = []string{methodLabel}
	writeBlockLabel = prometheus.Labels{
		methodLabel: "write_block",
	}
	readBlockLabel = prometheus.Labels{
		methodLabel: "read_block",
	}
	readHeaderLabel = prometheus.Labels{
		methodLabel: "read_header",
	}
	readBodyLabel = prometheus.Labels{
		methodLabel: "read_body",
	}
	hasBlockLabel = prometheus.Labels{
		methodLabel: "has_block",
	}
	closeLabel = prometheus.Labels{
		methodLabel: "close",
	}
)

// Database tracks the amount of time each operation takes and how many bytes
// are read/written to the underlying block database instance.
type Database struct {
	db database.BlockDatabase

	calls    *prometheus.CounterVec
	duration *prometheus.GaugeVec
	size     *prometheus.CounterVec
}

// New returns a new block database with added metrics
func New(
	reg prometheus.Registerer,
	namespace string,
	db database.BlockDatabase,
) (*Database, error) {
	meterDB := &Database{
		db: db,
		calls: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "calls",
				Help:      "number of calls to the block database",
			},
			methodLabels,
		),
		duration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "duration",
				Help:      "time spent in block database calls (ns)",
			},
			methodLabels,
		),
		size: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "size",
				Help:      "size of data passed in block database calls",
			},
			methodLabels,
		),
	}
	return meterDB, errors.Join(
		reg.Register(meterDB.calls),
		reg.Register(meterDB.duration),
		reg.Register(meterDB.size),
	)
}

func (db *Database) WriteBlock(height uint64, block []byte, headerSize uint32) error {
	start := time.Now()
	err := db.db.WriteBlock(height, block, headerSize)
	duration := time.Since(start)

	db.calls.With(writeBlockLabel).Inc()
	db.duration.With(writeBlockLabel).Add(float64(duration))
	db.size.With(writeBlockLabel).Add(float64(len(block)))
	return err
}

func (db *Database) ReadBlock(height uint64) ([]byte, error) {
	start := time.Now()
	block, err := db.db.ReadBlock(height)
	duration := time.Since(start)

	db.calls.With(readBlockLabel).Inc()
	db.duration.With(readBlockLabel).Add(float64(duration))
	if block != nil {
		db.size.With(readBlockLabel).Add(float64(len(block)))
	}
	return block, err
}

func (db *Database) ReadHeader(height uint64) ([]byte, error) {
	start := time.Now()
	header, err := db.db.ReadHeader(height)
	duration := time.Since(start)

	db.calls.With(readHeaderLabel).Inc()
	db.duration.With(readHeaderLabel).Add(float64(duration))
	if header != nil {
		db.size.With(readHeaderLabel).Add(float64(len(header)))
	}
	return header, err
}

func (db *Database) ReadBody(height uint64) ([]byte, error) {
	start := time.Now()
	body, err := db.db.ReadBody(height)
	duration := time.Since(start)

	db.calls.With(readBodyLabel).Inc()
	db.duration.With(readBodyLabel).Add(float64(duration))
	if body != nil {
		db.size.With(readBodyLabel).Add(float64(len(body)))
	}
	return body, err
}

func (db *Database) HasBlock(height uint64) (bool, error) {
	start := time.Now()
	has, err := db.db.HasBlock(height)
	duration := time.Since(start)

	db.calls.With(hasBlockLabel).Inc()
	db.duration.With(hasBlockLabel).Add(float64(duration))
	return has, err
}

func (db *Database) Close() error {
	start := time.Now()
	err := db.db.Close()
	duration := time.Since(start)

	db.calls.With(closeLabel).Inc()
	db.duration.With(closeLabel).Add(float64(duration))
	return err
}
