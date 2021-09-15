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
require("./commands_pb");

const http = require("http");
const {egressMessageBuilder, messageBuilder, StateFun, Context} = require("../dist/statefun");

// noinspection JSUnresolvedVariable
const SourceCommandType = StateFun.protoType("statefun.smoke.e2e/source-command", proto.org.apache.flink.statefun.e2e.smoke.SourceCommand);

// noinspection JSUnresolvedVariable
const CommandsType = StateFun.protoType("statefun.smoke.e2e/commands", proto.org.apache.flink.statefun.e2e.smoke.Commands);

// noinspection JSUnresolvedVariable
const VerificationResultType = StateFun.protoType("statefun.smoke.e2e/verification-result", proto.org.apache.flink.statefun.e2e.smoke.VerificationResult);

// noinspection JSValidateJSDoc
/**
 * @param {Context} context
 * @param {?proto.org.apache.flink.statefun.e2e.smoke.Command.Send} send
 */
function applySend(context, send) {
    context.send(messageBuilder({
        typename: "statefun.smoke.e2e/command-interpreter-fn",
        id: `${send.getTarget()}`,
        value: send.getCommands(),
        valueType: CommandsType
    }));
}

// noinspection JSValidateJSDoc
/**
 * @param {Context} context
 * @param {?proto.org.apache.flink.statefun.e2e.smoke.Command.IncrementState} increment
 */
function applyInc(context, increment) {
    context.storage.state += 1;
}

// noinspection JSValidateJSDoc
/**
 * @param {Context} context
 * @param {?proto.org.apache.flink.statefun.e2e.smoke.Command.SendAfter} sendAfter
 */
function applySendAfter(context, sendAfter) {
    context.sendAfter(1_000,
        messageBuilder({
            typename: "statefun.smoke.e2e/command-interpreter-fn",
            id: `${sendAfter.getTarget()}`,
            value: sendAfter.getCommands(),
            valueType: CommandsType
        }));
}

// noinspection JSValidateJSDoc
/**
 * @param {Context} context
 * @param {?proto.org.apache.flink.statefun.e2e.smoke.Command.SendEgress} sendEgress
 */
function applyEgress(context, sendEgress) {
    context.send(egressMessageBuilder({
        typename: "statefun.smoke.e2e/discard-sink",
        value: 'discarded-message',
        valueType: StateFun.stringType()
    }));
}

// noinspection JSValidateJSDoc
/**
 * @param {Context} context
 * @param {?proto.org.apache.flink.statefun.e2e.smoke.Command.Verify} verify
 */
function applyVerify(context, verify) {
    const id = context.self.id;
    const actual = context.storage.state;

    // noinspection JSUnresolvedVariable
    let result = new proto.org.apache.flink.statefun.e2e.smoke.VerificationResult();

    result.setActual(actual);
    result.setExpected(verify.getExpected());
    result.setId(parseInt(id));

    context.send(egressMessageBuilder({
        typename: "statefun.smoke.e2e/verification-sink",
        value: result,
        valueType: VerificationResultType
    }));
}

// noinspection JSValidateJSDoc
/**
 *
 * @param {Context} context
 * @param {proto.org.apache.flink.statefun.e2e.smoke.Commands} commands
 */
function applyCommands(context, commands) {
    for (let command of commands.getCommandList()) {
        if (command.hasSend()) {
            applySend(context, command.getSend());
        } else if (command.hasIncrement()) {
            applyInc(context, command.getIncrement());
        } else if (command.hasSendAfter()) {
            applySendAfter(context, command.getSendAfter());
        } else if (command.hasSendEgress()) {
            applyEgress(context, command.getSendEgress())
        } else if (command.hasVerify()) {
            applyVerify(context, command.getVerify());
        } else {
            throw new Error(`unknown command ${command}`);
        }
    }
}


let statefun = new StateFun();

statefun.bind({
    typename: "statefun.smoke.e2e/command-interpreter-fn",
    fn(context, message) {
        if (message.is(SourceCommandType)) {
            const sourceCommand = message.as(SourceCommandType);
            const commands = sourceCommand.getCommands();
            return applyCommands(context, commands);
        } else if (message.is(CommandsType)) {
            const commands = message.as(CommandsType)
            return applyCommands(context, commands);
        } else {
            throw new Error(`unknown message type ${message}`);
        }
    },
    specs: [{
        name: "state",
        type: StateFun.intType(),
    }
    ]
});

http.createServer(statefun.handler()).listen(8000);
