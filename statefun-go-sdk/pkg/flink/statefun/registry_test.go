// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statefun

import (
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/internal/messages"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialRegistration(t *testing.T) {
	var providedStates []*messages.ToFunction_PersistedValue

	statesSpecs := map[string]*persistedValue{
		"A": {
			schema: &messages.FromFunction_PersistedValueSpec{
				StateName: "A",
			},
		},
	}

	results := setRegisteredStates(providedStates, statesSpecs)
	assert.NotNil(t, results, "missing states should not be nil")
	assert.Equal(t, 1, len(results.GetIncompleteInvocationContext().MissingValues), "response should contain 1 missing value")
	assert.Equal(t, "A", results.GetIncompleteInvocationContext().MissingValues[0].StateName, "response should register state A")
}

func TestModifiedStateRegistration(t *testing.T) {
	providedStates := []*messages.ToFunction_PersistedValue{
		{
			StateName:  "B",
			StateValue: nil,
		},
	}

	statesSpecs := map[string]*persistedValue{
		"A": {
			schema: &messages.FromFunction_PersistedValueSpec{
				StateName: "A",
			},
		},
		"B": {
			schema: &messages.FromFunction_PersistedValueSpec{
				StateName: "B",
			},
		},
	}

	results := setRegisteredStates(providedStates, statesSpecs)
	assert.NotNil(t, results, "missing states should not be nil")
	assert.Equal(t, 1, len(results.GetIncompleteInvocationContext().MissingValues), "response should contain 1 missing value")
	assert.Equal(t, "A", results.GetIncompleteInvocationContext().MissingValues[0].StateName, "response should register state A")
}

func TestUnModifiedStateRegistration(t *testing.T) {

	providedStates := []*messages.ToFunction_PersistedValue{
		{
			StateName: "A",
		},
	}

	statesSpecs := map[string]*persistedValue{
		"A": {
			schema: &messages.FromFunction_PersistedValueSpec{
				StateName: "A",
			},
		},
	}

	results := setRegisteredStates(providedStates, statesSpecs)
	assert.Nil(t, results, "no response should be provided when all states are provided")
}
