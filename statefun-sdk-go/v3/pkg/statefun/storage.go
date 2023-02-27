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

package statefun

import (
	"fmt"
	"sync"

	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal"
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun/internal/protocol"
)

// An AddressScopedStorage is used for reading and writing persistent
// values that are managed by the Stateful Functions runtime for
// fault-tolerance and consistency.
//
// All access to the storage is scoped to the current function instance,
// identified by the instance's Address. This means that within an
// invocation, function instances may only access its own persisted
// values through this storage.
type AddressScopedStorage interface {

	// Get returns the values of the provided ValueSpec, scoped to the
	// current invoked Address and stores the result in the value
	// pointed to by receiver. The method will return false
	// if there is no value for the spec in storage
	// so callers can differentiate between missing and
	// the types zero value.
	Get(spec ValueSpec, receiver interface{}) (exists bool, err error)

	// Set updates the value for the provided ValueSpec, scoped
	// to the current invoked Address.
	Set(spec ValueSpec, value interface{}) error

	// Remove deletes the prior value set for the the provided
	// ValueSpec, scoped to the current invoked Address.
	//
	// After removing the value, calling Get for the same
	// spec under the same Address will return false.
	Remove(spec ValueSpec) error
}

type storage struct {
	mutex sync.RWMutex
	cells map[string]*internal.Cell
}

type storageFactory interface {
	getStorage() *storage

	getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec
}

func newStorageFactory(
	batch *protocol.ToFunction_InvocationBatchRequest,
	specs map[string]*protocol.FromFunction_PersistedValueSpec,
) storageFactory {
	storage := &storage{
		cells: make(map[string]*internal.Cell, len(specs)),
	}

	states := make(map[string]*protocol.FromFunction_PersistedValueSpec, len(specs))
	for k, v := range specs {
		states[k] = v
	}

	if batch.State != nil {
		for _, state := range batch.State {
			spec, exists := states[state.StateName]
			if !exists {
				continue
			}

			delete(states, state.StateName)

			storage.cells[state.StateName] = internal.NewCell(state, spec.TypeTypename)
		}
	}

	if len(states) > 0 {
		var missing = make([]*protocol.FromFunction_PersistedValueSpec, 0, len(states))
		for _, spec := range states {
			missing = append(missing, spec)
		}

		return MissingSpecs(missing)
	} else {
		return storage
	}
}

func (s *storage) getStorage() *storage {
	return s
}

func (s *storage) getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return nil
}

func (s *storage) Get(spec ValueSpec, receiver interface{}) (bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		return false, fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	if !cell.HasValue() {
		return false, nil
	}

	cell.SeekToBeginning()

	if err := spec.ValueType.Deserialize(cell, receiver); err != nil {
		return false, fmt.Errorf("failed to deserialize persisted value `%s`: %w", spec.Name, err)
	}

	return true, nil
}

func (s *storage) Set(spec ValueSpec, value interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		return fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	cell.Reset()

	if err := spec.ValueType.Serialize(cell, value); err != nil {
		return fmt.Errorf("failed to serialize %s: %w", spec.Name, err)
	}

	return nil
}

func (s *storage) Remove(spec ValueSpec) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		return fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	cell.Delete()

	return nil
}

func (s *storage) getStateMutations() []*protocol.FromFunction_PersistedValueMutation {
	mutations := make([]*protocol.FromFunction_PersistedValueMutation, 0, len(s.cells))
	for name, cell := range s.cells {
		if mutation := cell.GetStateMutation(name); mutation != nil {
			mutations = append(mutations, mutation)
		}
	}

	return mutations
}

type MissingSpecs []*protocol.FromFunction_PersistedValueSpec

func (m MissingSpecs) getStorage() *storage {
	return nil
}

func (m MissingSpecs) getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return m
}
