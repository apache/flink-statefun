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

package main

import (
	"github.com/apache/flink-statefun/statefun-sdk-go/v3/pkg/statefun"
	"log"
	"net/http"
)

var (
	appNamespace              = "statefun.smoke.e2e"
	commandFn, _              = statefun.TypeNameFromParts(appNamespace, "command-interpreter-fn")
	discardEgress, _          = statefun.TypeNameFromParts(appNamespace, "discard-sink")
	verificationEgress, _     = statefun.TypeNameFromParts(appNamespace, "verification-sink")
	commandsTypeName, _       = statefun.TypeNameFromParts(appNamespace, "commands")
	sourceCommandsTypeName, _ = statefun.TypeNameFromParts(appNamespace, "source-command")
	verificationTypeName, _   = statefun.TypeNameFromParts(appNamespace, "verification-result")
	commandsType              = statefun.MakeProtobufTypeWithTypeName(commandsTypeName)
	sourceCommandsType        = statefun.MakeProtobufTypeWithTypeName(sourceCommandsTypeName)
	verificationType          = statefun.MakeProtobufTypeWithTypeName(verificationTypeName)
)

func main() {
	spec := statefun.StatefulFunctionSpec{
		FunctionType: commandFn,
		States:       []statefun.ValueSpec{State},
		Function:     statefun.StatefulFunctionPointer(CommandInterpreterFn),
	}

	builder := statefun.StatefulFunctionsBuilder()
	_ = builder.WithSpec(spec)

	http.Handle("/", builder.AsHandler())
	log.Fatal(http.ListenAndServeTLS(":443", "/app/v3/test/smoketest/server.crt", "/app/v3/test/smoketest/server.key", nil))
}
