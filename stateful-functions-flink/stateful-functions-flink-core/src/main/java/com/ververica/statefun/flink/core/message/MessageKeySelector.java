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

package com.ververica.statefun.flink.core.message;

import com.ververica.statefun.flink.core.common.KeyBy;
import org.apache.flink.api.java.functions.KeySelector;

public final class MessageKeySelector implements KeySelector<Message, String> {

  private static final long serialVersionUID = 1;

  @Override
  public String getKey(Message value) {
    return KeyBy.apply(value.target());
  }
}
