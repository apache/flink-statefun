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

package com.ververica.statefun.state.processor.union;

import com.ververica.statefun.state.processor.BootstrapDataRouterProvider;
import java.util.Objects;
import org.apache.flink.api.java.DataSet;

/**
 * Represents a single registered bootstrap dataset, containing pre-tagged/routed bootstrap data
 * entries.
 */
public class BootstrapDataset<T> {

  private final DataSet<T> dataSet;
  private final BootstrapDataRouterProvider<T> routerProvider;

  public BootstrapDataset(DataSet<T> dataSet, BootstrapDataRouterProvider<T> routerProvider) {
    this.dataSet = Objects.requireNonNull(dataSet);
    this.routerProvider = Objects.requireNonNull(routerProvider);
  }

  DataSet<T> getDataSet() {
    return dataSet;
  }

  BootstrapDataRouterProvider<T> getRouterProvider() {
    return routerProvider;
  }
}
