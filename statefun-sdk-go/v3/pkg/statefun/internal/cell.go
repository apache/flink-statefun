// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
	"io"
	"log"
)

const (
	// smallBufferSize is an initial allocation minimal capacity.
	// this is the same constant as used in bytes.Buffer.
	smallBufferSize = 64
	maxInt          = int(^uint(0) >> 1)
)

// Cell is a mutable, persisted value.
// This struct is not thread safe.
type Cell struct {
	buf          []byte // contents are the bytes buf[off : len(buf)]
	off          int    // read at &buf[off], write at &buf[len(buf)]
	mutated      bool   // tracker if the cell has been mutated
	hasValue     bool   // tracker if the cell has a valid value
	typeTypeName string // the typename of the type whose serialized contents are stored in the cell
}

// NewCell creates and initializes a new Cell using state's value as its
// initial contents. The new Cell takes ownership of state, and the
// caller should not use state after this call.
func NewCell(state *protocol.ToFunction_PersistedValue, typeTypeName string) *Cell {
	c := &Cell{
		typeTypeName: typeTypeName,
	}

	if state.StateValue != nil && state.StateValue.HasValue {
		c.hasValue = true
		c.buf = state.StateValue.Value
	}

	return c
}

// SeekToBeginning resets the cell so the next
// read starts from the beginning of the underlying
// buffer, regardless of where the last read left off
func (c *Cell) SeekToBeginning() {
	c.off = 0
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered. Read is resumable
// and returns EOF when there are no more bytes to read. This behavior
// is required for Cell to interoperate with the go standard library.
// Users of Cell are required to call SeekToBeginning, before the first
// read to ensure reads always begin at the start of the buffer.
func (c *Cell) Read(p []byte) (n int, err error) {
	if c.empty() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}

// Reset resets the cell to be empty.
// This method must be called when
// setting a new value in storage
// to ensure the new value overrides
// the previous.
func (c *Cell) Reset() {
	c.buf = c.buf[:0]
	c.off = 0
}

// Write writes the given slice into the cell.
func (c *Cell) Write(p []byte) (n int, err error) {
	c.mutated = true
	c.hasValue = true

	// If buffer is empty, reset to recover space.
	if len(c.buf) == 0 && c.off != 0 {
		c.Reset()
	}

	m, ok := c.tryReslice(len(p))
	if !ok {
		m = c.grow(len(p))
	}
	return copy(c.buf[m:], p), nil
}

// Delete marks the value to be deleted and resets the cell to be empty,
func (c *Cell) Delete() {
	c.mutated = true
	c.hasValue = false
	c.Reset()
}

// HasValue returns true if the cell contains a valid value,
// if the value is false, calls to Read will consume 0 bytes
func (c *Cell) HasValue() bool {
	return c.hasValue
}

// GetStateMutation turns the final Cell into a FromFunction_PersistedValueMutation.
// The new FromFunction_PersistedValueMutation takes ownership of the underlying
// buffer and the cell should not be used after this function returns.
func (c *Cell) GetStateMutation(name string) *protocol.FromFunction_PersistedValueMutation {
	if !c.mutated {
		return nil
	}

	mutation := &protocol.FromFunction_PersistedValueMutation{
		MutationType: protocol.FromFunction_PersistedValueMutation_DELETE,
		StateName:    name,
	}

	if c.hasValue {
		mutation.MutationType = protocol.FromFunction_PersistedValueMutation_MODIFY

		mutation.StateValue = &protocol.TypedValue{
			Typename: c.typeTypeName,
			HasValue: true,
			Value:    c.buf,
		}
	}

	return mutation
}

// empty reports whether the unread portion of the cells buffer is empty.
func (c *Cell) empty() bool { return len(c.buf) <= c.off }

// len returns the number of bytes of the unread portion of the cells buffer
func (c *Cell) len() int { return len(c.buf) - c.off }

// tryReslice tries to reslice the cell, so it can
// fit n more bytes of data. It returns the index where bytes should
// be written and whether it succeeded.
func (c *Cell) tryReslice(n int) (int, bool) {
	if l := len(c.buf); n <= cap(c.buf)-l {
		c.buf = c.buf[:l+n]
		return l, true
	}
	return 0, false
}

// grow grows the cell to guarantee space for n more bytes.
// It returns the index where bytes should be written.
func (c *Cell) grow(n int) int {
	m := c.len()

	if c.buf == nil && n <= smallBufferSize {
		c.buf = make([]byte, n, smallBufferSize)
		return 0
	}
	capacity := cap(c.buf)
	if n <= capacity/2-m {
		// We can slide things down instead of allocating a new
		// slice. We only need m+n <= capacity to slide, but
		// we instead let capacity get twice as large so we
		// don't spend all our time copying.
		copy(c.buf, c.buf[c.off:])
	} else if capacity > maxInt-capacity-n {
		log.Panic("error: failed to allocate capacity for internal cell. required capacity is greater than maximum allocatable space")
	} else {
		buf := make([]byte, 2*capacity+n)
		copy(buf, c.buf[c.off:])
		c.buf = buf
	}

	c.off = 0
	c.buf = c.buf[:m+n]
	return m
}
