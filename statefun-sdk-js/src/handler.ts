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
'use strict';


import {Message} from "./message";

import "./generated/request-reply_pb";


import {Address, parseTypeName, ValueSpec} from "./core";
import {Context, InternalContext} from "./context";
import {AddressScopedStorageFactory} from "./storage";

const PB_ToFn = global.proto.io.statefun.sdk.reqreply.ToFunction;
const PB_FromFn = global.proto.io.statefun.sdk.reqreply.FromFunction
const PB_InvocationResponse = global.proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse;
const PB_Address = global.proto.io.statefun.sdk.reqreply.Address;

// ----------------------------------------------------------------------------------------------------
// Missing context handling
// ----------------------------------------------------------------------------------------------------

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 * @returns {!Uint8Array}
 */
function respondImmediatelyWithMissingContext(missing: ValueSpec[]): Uint8Array {
    const ctx = valueSpecsToIncompleteInvocationContext(missing);
    const pbFromFn = new PB_FromFn();
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
        const pbSpec = new PB_FromFn.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expireAfterWrite);
        // noinspection JSCheckFunctionSignatures,TypeScriptValidateJSTypes
        pbSpec.setMode(1) // AFTER_WRITE
        return pbSpec;
    }
    if (valueSpec.expireAfterCall !== -1) {
        const pbSpec = new PB_FromFn.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expireAfterCall);
        // noinspection JSCheckFunctionSignatures,TypeScriptValidateJSTypes
        pbSpec.setMode(2) // AFTER_CALL
        return pbSpec;
    }
    return null;
}

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 */
function valueSpecsToIncompleteInvocationContext(missing: ValueSpec[]) {
    const pbValueSpecs = missing.map(missingValueSpec => {
        let pbPersistedValueSpec = new PB_FromFn.PersistedValueSpec();

        pbPersistedValueSpec.setStateName(missingValueSpec.name);
        pbPersistedValueSpec.setTypeTypename(missingValueSpec.type.typename);

        const pbExpirationSpec = expirationSpecFromValueSpec(missingValueSpec);
        if (pbExpirationSpec !== null) {
            pbPersistedValueSpec.setExpirationSpec(pbExpirationSpec);
        }

        return pbPersistedValueSpec;
    });

    const res = new PB_FromFn.IncompleteInvocationContext();
    pbValueSpecs.forEach(pbSpec => res.addMissingValues(pbSpec));
    return res;
}

// ----------------------------------------------------------------------------------------------------
// Handler
// ----------------------------------------------------------------------------------------------------

async function tryHandle(toFunctionBytes: Buffer | Uint8Array, fns): Promise<Buffer | Uint8Array> {
    //
    // setup
    //
    const toFn = PB_ToFn.deserializeBinary(toFunctionBytes);
    const pbInvocationBatchRequest = toFn.getInvocation();
    const targetAddress = pbAddressToSdkAddress(pbInvocationBatchRequest.getTarget());
    const fnSpec = findTargetFunctionSpec(fns, targetAddress);
    const {missing, values, storage} = AddressScopedStorageFactory.tryCreateAddressScopedStorage(pbInvocationBatchRequest, fnSpec.valueSpecs);
    if (missing !== null) {
        return respondImmediatelyWithMissingContext(missing);
    }
    //
    // apply the batch
    //
    const internalContext = new InternalContext()
    const context = new Context(targetAddress, storage, internalContext);
    await applyBatch(pbInvocationBatchRequest, context, internalContext, fnSpec.fn);
    //
    // collect the side effects
    //
    let pbInvocationResponse = new PB_InvocationResponse();
    collectStateMutations(values, pbInvocationResponse);
    collectOutgoingMessages(internalContext.sent, pbInvocationResponse);
    collectEgress(internalContext.egress, pbInvocationResponse);
    collectDelayedMessage(internalContext.delayed, pbInvocationResponse);

    let fromFn = new PB_FromFn();
    fromFn.setInvocationResult(pbInvocationResponse)
    return fromFn.serializeBinary();
}

// noinspection JSValidateJSDoc
/**
 * @param {?proto.io.statefun.sdk.reqreply.ToFunction.InvocationBatchRequest} pbInvocationBatchRequest
 * @param {Context} context user facing context
 * @param {InternalContext} internalContext
 * @param {*} fn the function to apply
 */
async function applyBatch(pbInvocationBatchRequest, context: Context, internalContext: InternalContext, fn) {
    for (let invocation of pbInvocationBatchRequest.getInvocationsList()) {
        internalContext.caller = pbAddressToSdkAddress(invocation.getCaller());
        const message = new Message(context.self, invocation.getArgument());
        const maybePromise = fn(context, message);
        if (maybePromise instanceof Promise) {
            await maybePromise;
        }
    }
}

// ----------------------------------------------------------------------------------------------------
// Side Effect Collection
// ----------------------------------------------------------------------------------------------------

function collectStateMutations(values, pbInvocationResponse) {
    for (let mutation of AddressScopedStorageFactory.collectMutations(values)) {
        pbInvocationResponse.addStateMutations(mutation);
    }
}

function collectOutgoingMessages(sent, pbInvocationResponse) {
    for (let message of sent) {
        const pbAddr = sdkAddressToPbAddress(message.targetAddress);
        const pbArg = message.typedValue;

        let pbMessage = new PB_FromFn.Invocation();

        pbMessage.setTarget(pbAddr);
        pbMessage.setArgument(pbArg);
        pbInvocationResponse.addOutgoingMessages(pbMessage);
    }
}

function collectEgress(egresses, pbInvocationResponse) {
    for (let egress of egresses) {
        let outEgress = new PB_FromFn.EgressMessage();

        const {namespace, name} = parseTypeName(egress.typename);
        outEgress.setEgressNamespace(namespace);
        outEgress.setEgressType(name);
        outEgress.setArgument(egress.typedValue);

        pbInvocationResponse.addOutgoingEgresses(outEgress);
    }
}

function collectDelayedMessage(delayed, pbInvocationResponse) {
    for (let {type = "", delay = -1, token = "", what} of delayed) {
        let pb = new PB_FromFn.DelayedInvocation();
        if (type === 'send') {
            pb.setIsCancellationRequest(false);
            pb.setTarget(sdkAddressToPbAddress(what.targetAddress));
            pb.setDelayInMs(delay);
            pb.setArgument(what.typedValue);
        } else if (type === 'send_token') {
            pb.setIsCancellationRequest(false);
            pb.setTarget(sdkAddressToPbAddress(what.targetAddress));
            pb.setDelayInMs(delay);
            pb.setCancellationToken(token);
            pb.setArgument(what.typedValue);
        } else if (type === 'cancel') {
            pb.setIsCancellationRequest(true);
            pb.setCancellationToken(token);
        } else {
            throw new TypeError(`unknown delayed message type ${type}`);
        }
        pbInvocationResponse.addDelayedInvocations(pb);
    }
}

// ----------------------------------------------------------------------------------------------------
// Utils
// ----------------------------------------------------------------------------------------------------

function sdkAddressToPbAddress(sdkAddress) {
    let pbAddr = new PB_Address();
    pbAddr.setNamespace(sdkAddress.namespace);
    pbAddr.setType(sdkAddress.name);
    pbAddr.setId(sdkAddress.id);
    return pbAddr;
}

function pbAddressToSdkAddress(pbAddress) {
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
function findTargetFunctionSpec(fns, targetAddress) {
    if (!fns.hasOwnProperty(targetAddress.typename)) {
        throw new Error(`unknown function type ${targetAddress.typename}`);
    }
    return fns[targetAddress.typename];
}


export {tryHandle as handle}