// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package blockdb

import (
	"errors"
	"sync"
)

// Mock is an in-memory mock implementation of BlockDB for testing purposes
type Mock struct {
	mu                  sync.RWMutex
	blocks              map[BlockHeight]BlockData
	headerSizes         map[BlockHeight]BlockHeaderSize
	maxBlockHeight      BlockHeight
	maxContiguousHeight BlockHeight
	closed              bool
	config              DatabaseConfig
}

// NewMemoryDB creates a new in-memory BlockDB for testing
func NewMock() *Mock {
	return &Mock{
		blocks:              make(map[BlockHeight]BlockData),
		headerSizes:         make(map[BlockHeight]BlockHeaderSize),
		maxBlockHeight:      unsetHeight,
		maxContiguousHeight: unsetHeight,
		config:              DefaultConfig(),
	}
}

// WriteBlock stores a block in memory with the specified header size
func (m *Mock) WriteBlock(height BlockHeight, block BlockData, headerSize BlockHeaderSize) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrDatabaseClosed
	}

	if len(block) == 0 {
		return ErrBlockEmpty
	}

	if headerSize >= uint32(len(block)) {
		return ErrHeaderSizeTooLarge
	}

	m.blocks[height] = block
	m.headerSizes[height] = headerSize

	// Update max block height
	if height > m.maxBlockHeight || m.maxBlockHeight == unsetHeight {
		m.maxBlockHeight = height
	}

	// Update max contiguous height by checking if we can fill gaps
	m.updateMaxContiguousHeight()

	return nil
}

// updateMaxContiguousHeight recalculates the max contiguous height by checking for gaps
func (m *Mock) updateMaxContiguousHeight() {
	// Start from the current max contiguous height and check forward
	current := m.maxContiguousHeight
	if current == unsetHeight {
		// If no contiguous height yet, start from the minimum height
		// Handle the case where MinHeight can be 0
		if m.config.MinimumHeight > 0 {
			current = m.config.MinimumHeight - 1 // Start checking from one before the minimum
		} else {
			current = unsetHeight // Will be handled in the loop below
		}
	}

	// Check forward from current position to find the highest contiguous sequence
	nextHeightToVerify := m.config.MinimumHeight
	if current != unsetHeight {
		nextHeightToVerify = current + 1
	}

	for {
		if _, exists := m.blocks[nextHeightToVerify]; !exists {
			break // Found a gap, stop here
		}
		current = nextHeightToVerify
		nextHeightToVerify++
	}

	m.maxContiguousHeight = current
}

// ReadBlock retrieves the full block data for the given height
func (m *Mock) ReadBlock(height BlockHeight) (BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrDatabaseClosed
	}

	block, exists := m.blocks[height]
	if !exists {
		return nil, ErrBlockNotFound
	}

	return block, nil
}

// ReadHeader retrieves only the header portion of the block
func (m *Mock) ReadHeader(height BlockHeight) (BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrDatabaseClosed
	}

	block, exists := m.blocks[height]
	if !exists {
		return nil, ErrBlockNotFound
	}

	headerSize, exists := m.headerSizes[height]
	if !exists {
		return nil, ErrBlockNotFound
	}

	if headerSize == 0 {
		return nil, nil
	}

	if headerSize > uint32(len(block)) {
		return nil, errors.New("header size exceeds block size")
	}

	return block[:headerSize], nil
}

// ReadBody retrieves only the body portion of the block (excluding header)
func (m *Mock) ReadBody(height BlockHeight) (BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrDatabaseClosed
	}

	block, exists := m.blocks[height]
	if !exists {
		return nil, ErrBlockNotFound
	}

	headerSize, exists := m.headerSizes[height]
	if !exists {
		return nil, ErrBlockNotFound
	}

	if headerSize >= uint32(len(block)) {
		return nil, errors.New("header size exceeds or equals block size")
	}

	return block[headerSize:], nil
}

// MaxContiguousHeight returns the highest block height known to be contiguously stored
func (m *Mock) MaxContiguousHeight() (BlockHeight, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.maxContiguousHeight == unsetHeight {
		return 0, false
	}
	return m.maxContiguousHeight, true
}

// HasBlock checks if a block exists at the given height
func (m *Mock) HasBlock(height BlockHeight) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return false, ErrDatabaseClosed
	}

	_, exists := m.blocks[height]
	return exists, nil
}

// Close closes the in-memory database
func (m *Mock) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	m.blocks = nil
	m.headerSizes = nil
	return nil
}
