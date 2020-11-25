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

package main

import (
	"context"
	"fmt"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun"
	"github.com/apache/flink-statefun/statefun-go-sdk/pkg/flink/statefun/io"
	"google.golang.org/protobuf/types/known/anypb"
	"net/http"
)

var egressId = io.EgressIdentifier{
	EgressNamespace: "example",
	EgressType:      "greets",
}

type Greeter struct {
	SeenCount statefun.State `name:"seen_count"`
}

func (g Greeter) Invoke(ctx context.Context, runtime statefun.Output, _ *anypb.Any) error {
	var seen SeenCount

	if _, err := g.SeenCount.Get(&seen); err != nil {
		return err
	}

	seen.Seen += 1

	if err := g.SeenCount.Set(&seen); err != nil {
		return err
	}

	self := statefun.Self(ctx)
	response := computeGreeting(self.Id, seen.Seen)

	record := io.KafkaRecord{
		Topic: "greetings",
		Key:   statefun.Self(ctx).Id,
		Value: response,
	}

	message, err := record.ToMessage()
	if err != nil {
		return nil
	}

	return runtime.SendEgress(egressId, message)
}

func computeGreeting(id string, seen int64) *GreetResponse {
	templates := []string{
		"",
		"Welcome %s",
		"Nice to see you again %s",
		"Third time is a charm %s"}

	greeting := &GreetResponse{}
	if int(seen) < len(templates) {
		greeting.Greeting = fmt.Sprintf(templates[seen], id)
	} else {
		greeting.Greeting = fmt.Sprintf("Nice to see you at the %d-nth time %s!", seen, id)
	}

	return greeting
}

func main() {
	registry := statefun.NewFunctionRegistry()
	_ = registry.RegisterFunction(statefun.FunctionType{
		Namespace: "example",
		Type:      "greeter",
	}, &Greeter{})

	http.Handle("/statefun", registry)
	_ = http.ListenAndServe(":8000", nil)
}
