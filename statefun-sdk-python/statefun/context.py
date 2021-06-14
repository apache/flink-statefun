################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import abc
import typing
from datetime import timedelta

from statefun.core import SdkAddress
from statefun.messages import Message, EgressMessage


class Context(abc.ABC):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def address(self) -> SdkAddress:
        """

        :return: the address of the currently executing function. the address is of the form (typename, id)
        """
        pass

    @property
    @abc.abstractmethod
    def storage(self):
        """

        :return: the address scoped storage.
        """
        pass

    @property
    @abc.abstractmethod
    def caller(self) -> typing.Union[None, SdkAddress]:
        """

        :return: the address of the caller or None if this function was triggered by the ingress.
        """
        pass

    def send(self, message: Message):
        """
        Send a message to a function.

        :param message: a message to send.
        """
        pass

    def send_after(self, duration: timedelta, message: Message, cancellation_token: str = ""):
        """
        Send a message to a target function after a specified delay.

        :param duration: the amount of time to wait before sending this message out.
        :param message: the message to send.
        :param cancellation_token: an optional cancellation token to associate with this message.
        """
        pass

    def cancel_delayed_message(self, cancellation_token: str):
        """
        Cancel a delayed message (message that was sent using send_after) with a given token.

        Please note that this is a best-effort operation, since the message might have been already delivered.
        If the message was delivered, this is a no-op operation.
        """
        pass

    def send_egress(self, message: EgressMessage):
        """
        Send a message to an egress.

        :param message: the EgressMessage to send.
        """
        pass
