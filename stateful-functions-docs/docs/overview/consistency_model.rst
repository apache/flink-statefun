.. Copyright 2019 Ververica GmbH.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

.. _consistency_model:

#################
Consistency Model
#################

.. contents:: :local:

**Stateful Functions** provides a high-level of consistency by following the pattern of function/state co-location and single reader/write access — a `well known <https://cs.brown.edu/courses/cs227/archives/2012/papers/weaker/cidr07p15.pdf>`_ approach that underlies many modern stream processors, databases and other systems. In combination with exactly-once state/messaging semantics, it forms the foundation for a strong consistency model:

.. topic:: Up-to-date function state

	Each function state always reflects previous updates. The function is the single reader/writer to its state, and both are (logically) co-located. Function state seems instantaneously persistent, not relying on the completion of a remote operation.

.. topic:: No loss and no duplication of messages

	Functions message each other with exactly-once semantics. Exactly-once messaging here means that functions perceive messages as being delivered exactly one time.

.. topic:: Failover safety

	If any state update or messaging delivery actually fails (in case of a failure/recovery), the state and messages are rolled back consistently across multiple functions to a state from prior to that failed operation. The logic is then re-attempted. That way functions get the impression of failure-free execution without message loss and keep the state of all functions consistent with each other.

Application developers still need to “think message-based” and deal with multiple intermediate states — for example, when requiring multiple input messages for an action or round-trip messaging with other functions. But they can drop a lot of the complex “defensive logic” against lost messages, broken connections, timeouts, concurrent modifications of shared state and other common setbacks.

.. warning::
	Some care needs to be taken when to communicate the effects to components outside a **Stateful Functions** application. Similar to the topic of “manifesting side-effects” in stream processing, there exist solutions, like transactional egresses. These may however impact the latency of propagating that informing. 

Consistency vs. Availability Trade-off
======================================

There is an inherent trade-off between the level of availability and consistency that an application can provide. The `CAP theorem <https://en.wikipedia.org/wiki/CAP_theorem>`_ famously states that trade-off (though in a simplified manner without all the `nuances <https://jepsen.io/consistency>`_). There is no right or wrong in the trade-off you choose: systems that pick different trade-offs target different use cases.

We picked a high consistency level for **Stateful Functions** and, while it `cannot be totally available <http://www.bailis.org/papers/hat-vldb2014.pdf>`_, we believe it is the right choice, because:

* It makes application development much easier and much more accessible. In practice, many development teams struggle heavily in the presence of lower consistency guarantees.

* It is still possible to provide very high levels of availability with this consistency model. For example, Google Spanner solves an arguable harder consistency/isolation problem and still managed to give such high availability that it was by all practical considerations `as good as totally available <https://cloud.google.com/blog/products/gcp/inside-cloud-spanner-and-the-cap-theorem>`_. And while not all software can assume atomic clocks and or rely on well-behaved networks `a lot can be achieved in pure software <http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf>`_. Some more development work is arguable needed on the runtime side, because Apache Flink currently favors higher throughput and more coarse-grained durability, but we believe in this overall direction.

* High consistency is the necessary foundation for even more powerful building blocks, like offering protocols/behaviors with transactional isolation between modifications of multiple function states. This can be implemented on top of streaming messaging, as `illustrated in this example <https://www.ververica.com/hubfs/Ververica/Docs/%5B2018-08%5D-dA-Streaming-Ledger-whitepaper.pdf>`_. Without high consistency, stronger isolation levels between concurrent transactions have much less useful semantics.
