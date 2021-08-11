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

// A StatefulFunction is a user-defined function that can be invoked with a given input.
// This is the primitive building block for a Stateful Functions application.
//
// Concept
//
// Each individual StatefulFunction is an uniquely invokable "instance" of a registered
// StatefulFunctionSpec. Each instance is identified by an Address, representing the
// function's unique id (a string) within its type. From a user's perspective, it would seem as if
// for each unique function id, there exists a stateful instance of the function that is always
// available to be invoked within a Stateful Functions application.
//
// Invoking a StatefulFunction
//
// An individual StatefulFunction can be invoked with arbitrary input from any another
// StatefulFunction (including itself), or routed from ingresses. To invoke a
// StatefulFunction, the caller simply needs to know the Address of the target function.
//
// As a result of invoking a StatefulFunction, the function may continue to invoke other
// functions, access persisted values, or send messages to egresses.
//
// Persistent State
//
// Each individual StatefulFunction may have persistent values written to storage that is
// maintained by the system, providing consistent exactly-once and fault-tolerant guarantees. Please
// see docs in ValueSpec and AddressScopedStorage for an overview of how to
// register persistent values and access the storage.
type StatefulFunction interface {

	// Invoke is the method called for each message. The passed Context
	// is canceled as soon as Invoke returns as a signal to
	// any spawned go routines. The method may return
	// an Error to signal the invocation failed and should
	// be reattempted.
	Invoke(ctx Context, message Message) error
}

// StatefulFunctionSpec for a Stateful Function, identifiable
// by a unique TypeName.
type StatefulFunctionSpec struct {
	// The unique TypeName associated
	// the StatefulFunction being defined.
	FunctionType TypeName

	// A slice of registered ValueSpec's that will be used
	// by this function. A function may only access values
	// that have been eagerly registered as part of its spec.
	States []ValueSpec

	// The physical StatefulFunction instance.
	Function StatefulFunction
}

// The StatefulFunctionPointer type is an adapter to allow the use of
// ordinary functions as StatefulFunction's. If f is a function
// with the appropriate signature, StatefulFunctionPointer(f) is a
// StatefulFunction that calls f.
type StatefulFunctionPointer func(Context, Message) error

func (s StatefulFunctionPointer) Invoke(ctx Context, message Message) error {
	return s(ctx, message)
}
