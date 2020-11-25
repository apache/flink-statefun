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

type Function struct {
	A State `name:"state-a"`
	B State `name:"state-b" expireAfterWrite:"1m"`
	C State `name:"state-c" expireAfterInvoke:"1m"`
}

func TestStateTagParse(t *testing.T) {
	f := Function{}
	fields, err := getStateFields(&f)

	assert.NoError(t, err, "failed to parse struct")

	assert.NotNil(t, f.A, "failed to set value for field A")
	assert.Contains(t, fields, "state-a", "missing `state-a`")
	assert.Equal(t, fields["state-a"].schema, &messages.FromFunction_PersistedValueSpec{
		StateName: "state-a",
	})

	assert.NotNil(t, f.B, "failed to set value for field B")
	assert.Contains(t, fields, "state-b", "missing `state-b`")
	assert.Equal(t, fields["state-b"].schema, &messages.FromFunction_PersistedValueSpec{
		StateName: "state-b",
		ExpirationSpec: &messages.FromFunction_ExpirationSpec{
			Mode:              messages.FromFunction_ExpirationSpec_AFTER_WRITE,
			ExpireAfterMillis: 60000,
		},
	})

	assert.NotNil(t, f.C, "failed to set value for field C")
	assert.Contains(t, fields, "state-c", "missing `state-c`")
	assert.Equal(t, fields["state-c"].schema, &messages.FromFunction_PersistedValueSpec{
		StateName: "state-c",
		ExpirationSpec: &messages.FromFunction_ExpirationSpec{
			Mode:              messages.FromFunction_ExpirationSpec_AFTER_INVOKE,
			ExpireAfterMillis: 60000,
		},
	})
}

func TestWrongType(t *testing.T) {
	_, err := getStateFields(Function{})
	assert.Error(t, err, "non-pointer struct should error")

	_, err = getStateFields("")
	assert.Error(t, err, "struct type should error")
}
