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
	"errors"
	"fmt"
)

// CancellationToken tags a delayed message send with statefun.SendAfterWithCancellationToken.
// It can then be used to cancel said message on a best effort basis with statefun.CancelDelayedMessage.
// The underlying string token can be retrieved by invoking Token().
type CancellationToken interface {
	fmt.Stringer

	// Token returns the underlying string
	// used to create the CancellationToken.
	Token() string

	// prevents external implementations
	// of the interface.
	internal()
}

type token string

func (t token) String() string {
	return "CancellationToken(" + string(t) + ")"
}

func (t token) Token() string {
	return string(t)
}

func (t token) internal() {}

// NewCancellationToken creates a new cancellation token or
// returns an error if the token is invalid.
func NewCancellationToken(t string) (CancellationToken, error) {
	if len(t) == 0 {
		return nil, errors.New("cancellation token cannot be empty")
	}
	return token(t), nil
}
