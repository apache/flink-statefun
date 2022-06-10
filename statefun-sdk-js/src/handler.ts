/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"use strict";

import {EgressMessage, Message} from "./message";

import "./generated/request-reply_pb";


import {Address, FunctionSpec, JsStatefulFunction, parseTypeName, ValueSpec} from "./core";
import {CancellationRequest, Context, DelayedMessage, InternalContext} from "./context";
import {AddressScopedStorageFactory, Value} from "./storage";

// ----------------------------------------------------------------------------------------------------
// Missing context handling
// ----------------------------------------------------------------------------------------------------

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 * @returns {!Uint8Array}
 */
function respondImmediatelyWithMissingContext(missing: ValueSpec[]): Uint8Array {
    const ctx = valueSpecsToIncompleteInvocationContext(missing);
    const pbFromFn = new proto.io.statefun.sdk.reqreply.FromFunction();
    pbFromFn.setIncompleteInvocationContext(ctx);
    return pbFromFn.serializeBinary();
}

// noinspection JSValidateJSDoc
/**
 * @param {ValueSpec} valueSpec the input value spec.
 * @returns {null|proto.io.statefun.sdk.reqreply.FromFunction.ExpirationSpec} an expiration spec if one is set, or null otherwise.
 */
function expirationSpecFromValueSpec(valueSpec: ValueSpec) {
    if (valueSpec.expireAfterWrite !== -1) {
        const pbSpec = new proto.io.statefun.sdk.reqreply.FromFunction.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expireAfterWrite);
        // noinspection JSCheckFunctionSignatures,TypeScriptValidateJSTypes
        pbSpec.setMode(1); // AFTER_WRITE
        return pbSpec;
    }
    if (valueSpec.expireAfterCall !== -1) {
        const pbSpec = new proto.io.statefun.sdk.reqreply.FromFunction.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expireAfterCall);
        // noinspection JSCheckFunctionSignatures,TypeScriptValidateJSTypes
        pbSpec.setMode(2); // AFTER_CALL
        return pbSpec;
    }
    return null;
}

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 */
function valueSpecsToIncompleteInvocationContext(missing: ValueSpec[]) {
    const pbValueSpecs = missing.map(missingValueSpec => {
        const pbPersistedValueSpec = new proto.io.statefun.sdk.reqreply.FromFunction.PersistedValueSpec();

        pbPersistedValueSpec.setStateName(missingValueSpec.name);
        pbPersistedValueSpec.setTypeTypename(missingValueSpec.type.typename);

        const pbExpirationSpec = expirationSpecFromValueSpec(missingValueSpec);
        if (pbExpirationSpec !== null) {
            pbPersistedValueSpec.setExpirationSpec(pbExpirationSpec);
        }

        return pbPersistedValueSpec;
    });

    const res = new proto.io.statefun.sdk.reqreply.FromFunction.IncompleteInvocationContext();
    pbValueSpecs.forEach(pbSpec => res.addMissingValues(pbSpec));
    return res;
}

// ----------------------------------------------------------------------------------------------------
// Handler
// ----------------------------------------------------------------------------------------------------

export async function handle(
    toFunctionBytes: Buffer | Uint8Array,
    fns: Record<string, FunctionSpec>
): Promise<Buffer | Uint8Array> {
    //
    // setup
    //
    const toFn = proto.io.statefun.sdk.reqreply.ToFunction.deserializeBinary(toFunctionBytes);
    const pbInvocationBatchRequest = toFn.getInvocation();
    if (pbInvocationBatchRequest === null) {
        throw new Error("An empty invocation request");
    }
    const targetAddress = pbAddressToSdkAddress(pbInvocationBatchRequest.getTarget());
    if (targetAddress === null) {
        throw new Error("Missing target address");
    }
    const fnSpec = findTargetFunctionSpec(fns, targetAddress);
    const {missing, values, storage} = AddressScopedStorageFactory.tryCreateAddressScopedStorage(pbInvocationBatchRequest, fnSpec.valueSpecs);
    if (missing !== null) {
        return respondImmediatelyWithMissingContext(missing);
    }
    if (values === undefined || values === null) {
        throw new Error("Unexpected internal error. Could not create an address scoped storage.");
    }
    //
    // apply the batch
    //
    const internalContext = new InternalContext();
    const context = new Context(targetAddress, storage, internalContext);
    await applyBatch(pbInvocationBatchRequest, context, internalContext, fnSpec.fn);
    //
    // collect the side effects
    //
    const pbInvocationResponse = new proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse();
    collectStateMutations(values, pbInvocationResponse);
    collectOutgoingMessages(internalContext.sent, pbInvocationResponse);
    collectEgress(internalContext.egress, pbInvocationResponse);
    collectDelayedMessage(internalContext.delayed, pbInvocationResponse);

    const fromFn = new proto.io.statefun.sdk.reqreply.FromFunction();
    fromFn.setInvocationResult(pbInvocationResponse);
    return fromFn.serializeBinary();
}

// noinspection JSValidateJSDoc
/**
 * @param {?proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest} pbInvocationBatchRequest
 * @param {Context} context user facing context
 * @param {InternalContext} internalContext
 * @param {*} fn the function to apply
 */
async function applyBatch(
    pbInvocationBatchRequest: proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest,
    context: Context,
    internalContext: InternalContext,
    fn: JsStatefulFunction
) {
    for (const invocation of pbInvocationBatchRequest.getInvocationsList()) {
        internalContext.caller = pbAddressToSdkAddress(invocation.getCaller());
        const message = new Message(context.self, invocation.getArgument()!);
        const maybePromise = fn(context, message);
        if (maybePromise instanceof Promise) {
            await maybePromise;
        }
    }
}

// ----------------------------------------------------------------------------------------------------
// Side Effect Collection
// ----------------------------------------------------------------------------------------------------

function collectStateMutations(
    values: Value<unknown>[],
    pbInvocationResponse: proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse
) {
    for (const mutation of AddressScopedStorageFactory.collectMutations(values)) {
        pbInvocationResponse.addStateMutations(mutation);
    }
}

function collectOutgoingMessages(
    sent: Message[],
    pbInvocationResponse: proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse
) {
    for (const message of sent) {
        const pbAddr = sdkAddressToPbAddress(message.targetAddress);
        const pbArg = message.typedValue;

        const pbMessage = new proto.io.statefun.sdk.reqreply.FromFunction.Invocation();

        pbMessage.setTarget(pbAddr);
        pbMessage.setArgument(pbArg);
        pbInvocationResponse.addOutgoingMessages(pbMessage);
    }
}

function collectEgress(
    egresses: EgressMessage[],
    pbInvocationResponse: proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse
) {
    for (const egress of egresses) {
        const outEgress = new proto.io.statefun.sdk.reqreply.FromFunction.EgressMessage();

        const {namespace, name} = parseTypeName(egress.typename);
        outEgress.setEgressNamespace(namespace);
        outEgress.setEgressType(name);
        outEgress.setArgument(egress.typedValue);

        pbInvocationResponse.addOutgoingEgresses(outEgress);
    }
}

function collectDelayedMessage(
    delayed: (DelayedMessage | CancellationRequest)[],
    pbInvocationResponse: proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse
) {
    for (const delayedOr of delayed) {
        const pb = new proto.io.statefun.sdk.reqreply.FromFunction.DelayedInvocation();
        if (delayedOr instanceof CancellationRequest) {
            pb.setIsCancellationRequest(true);
            pb.setCancellationToken(delayedOr.token);
        } else {
            pb.setIsCancellationRequest(false);
            pb.setTarget(sdkAddressToPbAddress(delayedOr.message.targetAddress));
            pb.setDelayInMs(delayedOr.delay);
            pb.setArgument(delayedOr.message.typedValue);
            if (delayedOr.token !== undefined) {
                pb.setCancellationToken(delayedOr.token);
            }
        }
        pbInvocationResponse.addDelayedInvocations(pb);
    }
}

// ----------------------------------------------------------------------------------------------------
// Utils
// ----------------------------------------------------------------------------------------------------

function sdkAddressToPbAddress(sdkAddress: Address) {
    const pbAddr = new proto.io.statefun.sdk.reqreply.Address();
    pbAddr.setNamespace(sdkAddress.namespace);
    pbAddr.setType(sdkAddress.name);
    pbAddr.setId(sdkAddress.id);
    return pbAddr;
}

function pbAddressToSdkAddress(pbAddress: proto.io.statefun.sdk.reqreply.Address | null | undefined) {
    if (pbAddress === undefined || pbAddress === null) {
        return null;
    }
    return Address.fromParts(pbAddress.getNamespace(), pbAddress.getType(), pbAddress.getId());
}

/**
 *
 * @param { {string : FunctionSpec} } fns an index of function specs by function typename.
 * @param {Address} targetAddress the target function address which we need to invoke.
 * @returns {FunctionSpec} the function spec that this batch is addressed to.
 */
function findTargetFunctionSpec(fns: Record<string, FunctionSpec>, targetAddress: Address): FunctionSpec {
    if (!fns.hasOwnProperty(targetAddress.typename)) {
        throw new Error(`unknown function type ${targetAddress.typename}`);
    }
    return fns[targetAddress.typename];
}
