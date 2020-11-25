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

package errors

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// StateFunError represents one or more details about an error. They are usually
// nested in the order that additional context was wrapped around the original
// error.
type StateFunError struct {
	code    int
	cause   error
	message string
}

// BadRequest returns an error that indicates
// bad data from the StateFun runtime.
// Each call to New returns a distinct error value even if the text is identical.
func BadRequest(format string, args ...interface{}) error {
	return &StateFunError{
		code:  http.StatusBadRequest,
		cause: fmt.Errorf(format, args...),
	}
}

// New returns an error that formats as the given text.
// Each call to New returns a distinct error value even if the text is identical.
func New(format string, args ...interface{}) error {
	return &StateFunError{
		code:    http.StatusInternalServerError,
		cause:   nil,
		message: fmt.Sprintf(format, args...),
	}
}

// Wrap returns a new error annotating err with a new message according to
// the format specifier.
func Wrap(e error, format string, args ...interface{}) error {
	code := http.StatusInternalServerError
	if inner, ok := e.(*StateFunError); ok {
		code = inner.code
	}

	return &StateFunError{
		code:    code,
		cause:   e,
		message: fmt.Sprintf(format, args...),
	}
}

// ToCode translates the error
// into an http error code.
func ToCode(e error) int {
	switch unwrapped := e.(type) {
	case *StateFunError:
		return unwrapped.code
	default:
		return http.StatusInternalServerError
	}
}

// Error outputs a beamError as a string. The top-level error message is
// displayed first, followed by each error's context and error message in
// sequence. The original error is output last.
func (e *StateFunError) Error() string {
	var builder strings.Builder

	e.printRecursive(&builder)

	return builder.String()
}

// printRecursive is a helper function for outputting the contexts and messages
// of a sequence of beamErrors.
func (e *StateFunError) printRecursive(builder *strings.Builder) {
	wraps := e.cause != nil

	if e.message != "" {
		builder.WriteString(e.message)
		if wraps {
			builder.WriteString("\n\tcaused by:\n")
		}
	}

	if wraps {
		if be, ok := e.cause.(*StateFunError); ok {
			be.printRecursive(builder)
		} else {
			builder.WriteString(e.cause.Error())
		}
	}
}

// Format implements the fmt.Formatter interface
func (e *StateFunError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v', 's':
		_, _ = io.WriteString(s, e.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.Error())
	}
}
