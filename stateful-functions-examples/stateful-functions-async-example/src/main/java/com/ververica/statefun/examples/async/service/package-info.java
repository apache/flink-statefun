/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This package represents a fictions service that needs to compute something asynchronously. In
 * this example, a query service keeps tack of tasks that are executed elsewhere (perhaps some
 * worker pool outside of the application) see: {@link
 * com.ververica.statefun.examples.async.service.TaskQueryService}. In reality, functions might want
 * to compute something asynchronously like sending an http request, querying a remote database, or
 * anything really that needs some time to complete.
 */
package com.ververica.statefun.examples.async.service;
