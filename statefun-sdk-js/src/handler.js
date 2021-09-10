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

require("./generated/request-reply_pb")

const {Message} = require("./message");
const {Address, parseTypeName} = require("./core");
const {Context} = require("./context");
const {AddressScopedStorageFactory} = require("./storage");

// noinspection JSUnresolvedVariable
const PB_ToFn = proto.io.statefun.sdk.reqreply.ToFunction;

// noinspection JSUnresolvedVariable
const PB_FromFn = proto.io.statefun.sdk.reqreply.FromFunction

// noinspection JSUnresolvedVariable
const PB_InvocationResponse = proto.io.statefun.sdk.reqreply.FromFunction.InvocationResponse;

// noinspection JSUnresolvedVariable
const PB_Address = proto.io.statefun.sdk.reqreply.Address;

// ----------------------------------------------------------------------------------------------------
// Missing context handling
// ----------------------------------------------------------------------------------------------------

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 * @returns {!Uint8Array}
 */
function respondImmediatelyWithMissingContext(missing) {
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
function expirationSpecFromValueSpec(valueSpec) {
    if (valueSpec.expire_after_write !== -1) {
        const pbSpec = new PB_FromFn.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expire_after_write);
        // noinspection JSCheckFunctionSignatures
        pbSpec.setMode(1) // AFTER_WRITE
        return pbSpec;
    }
    if (valueSpec.expire_after_call !== -1) {
        const pbSpec = new PB_FromFn.ExpirationSpec();
        pbSpec.setExpireAfterMillis(valueSpec.expire_after_call);
        // noinspection JSCheckFunctionSignatures
        pbSpec.setMode(2) // AFTER_CALL
        return pbSpec;
    }
    return null;
}

/**
 * @param {[ValueSpec]} missing a list of value spec that the server does not know about.
 */
function valueSpecsToIncompleteInvocationContext(missing) {
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

async function tryHandle(toFunctionBytes, fns) {
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
    const context = new Context(targetAddress, null, storage);
    await applyBatch(pbInvocationBatchRequest, context, fnSpec.fn);
    //
    // collect the side effects
    //
    let pbInvocationResponse = new PB_InvocationResponse();
    collectStateMutations(values, pbInvocationResponse);
    collectOutgoingMessages(context, pbInvocationResponse);
    collectEgress(context, pbInvocationResponse);
    collectDelayedMessage(context, pbInvocationResponse);

    let fromFn = new PB_FromFn();
    fromFn.setInvocationResult(pbInvocationResponse)
    return fromFn.serializeBinary();
}

async function applyBatch(pbInvocationBatchRequest, context, fn) {
    for (let invocation of pbInvocationBatchRequest.getInvocationsList()) {
        context.caller = pbAddressToSdkAddress(invocation.getCaller());
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

function collectOutgoingMessages(context, pbInvocationResponse) {
    for (let message of context.sent) {
        const pbAddr = sdkAddressToPbAddress(message.targetAddress);
        const pbArg = message.typedValue;

        let pbMessage = new PB_FromFn.Invocation();

        pbMessage.setTarget(pbAddr);
        pbMessage.setArgument(pbArg);
        pbInvocationResponse.addOutgoingMessages(pbMessage);
    }
}

function collectEgress(context, pbInvocationResponse) {
    for (let egress of context.egresses) {
        let outEgress = new PB_FromFn.EgressMessage();

        const {namespace, name} = parseTypeName(egress.typename);
        outEgress.setEgressNamespace(namespace);
        outEgress.setEgressType(name);
        outEgress.setArgument(egress.typedValue);

        pbInvocationResponse.addOutgoingEgresses(outEgress);
    }
}

function collectDelayedMessage(context, pbInvocationResponse) {
    for (let {type = "", delay = -1, token = "", what} of context.delayed) {
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


module.exports.handle = tryHandle;