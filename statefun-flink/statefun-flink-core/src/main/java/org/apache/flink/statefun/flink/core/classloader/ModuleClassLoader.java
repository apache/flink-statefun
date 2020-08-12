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
package org.apache.flink.statefun.flink.core.classloader;

import static java.util.Collections.synchronizedMap;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.spi.Constants;
import org.apache.flink.statefun.flink.core.spi.ModuleSpecs;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adds user modules onto the provided classloader. */
public class ModuleClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ModuleClassLoader.class);

  // Maintain weak references to both the keys and values. This is because
  // the module classloader (value) contains a reference to the parent class
  // classloader (key) and so the value must be reachable by the garbage
  // collector.
  private static final Map<ClassLoader, WeakReference<ClassLoader>> classloaders =
      synchronizedMap(new WeakHashMap<>());

  private static final String[] ALWAYS_PARENT_FIRST =
      new String[] {"org.apache.flink.statefun", "org.apache.kafka", "com.google.protobuf"};

  public static ClassLoader createModuleClassLoader(
      StatefulFunctionsConfig config, ClassLoader parent) {
    if (config.isModuleClassPathEnabled()) {
      return classloaders.compute(parent, ModuleClassLoader::getOrCreateClassLoader).get();
    } else {
      return parent;
    }
  }

  @Nonnull
  private static WeakReference<ClassLoader> getOrCreateClassLoader(
      ClassLoader parent, WeakReference<ClassLoader> ref) {
    if (ref == null || ref.get() == null) {
      ClassLoader cl =
          FlinkUserCodeClassLoaders.childFirst(
              obtainModuleAdditionalClassPath(),
              parent,
              ALWAYS_PARENT_FIRST,
              FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER);

      ref = new WeakReference<>(cl);
    }

    return ref;
  }

  private static URL[] obtainModuleAdditionalClassPath() {
    try {
      ModuleSpecs specs = ModuleSpecs.fromPath(Constants.MODULE_DIRECTORY);
      List<URL> classPath = new ArrayList<>();
      for (ModuleSpecs.ModuleSpec spec : specs) {
        for (URI uri : spec.artifactUris()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding {} to the module classpath", uri.toString());
          }
          classPath.add(uri.toURL());
        }
      }
      return classPath.toArray(new URL[0]);
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to load modules from path " + Constants.MODULE_DIRECTORY, e);
    }
  }
}
