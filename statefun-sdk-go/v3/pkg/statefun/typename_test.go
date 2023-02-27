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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeNameParse(t *testing.T) {
	typename, err := ParseTypeName("namespace/tpe")

	assert.NoError(t, err)
	assert.Equal(t, typename.GetNamespace(), "namespace")
	assert.Equal(t, typename.GetType(), "tpe")
}

func TestNoNamespace(t *testing.T) {
	_, err := ParseTypeName("/bar")
	assert.Error(t, err)
}

func TestNoName(t *testing.T) {
	_, err := ParseTypeName("n/")
	assert.Error(t, err)
}

func TestNoNamespaceOrName(t *testing.T) {
	_, err := ParseTypeName("/")
	assert.Error(t, err)
}

func TestEmptyString(t *testing.T) {
	_, err := ParseTypeName("")
	assert.Error(t, err)
}
