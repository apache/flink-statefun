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
package com.ververica.statefun.flink.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;

public final class ResourceLocator {
  private ResourceLocator() {}

  /** Locates a resource with a given name in the classpath or url path. */
  public static Iterable<URL> findNamedResources(String name) {
    URI nameUri = URI.create(name);
    if (!isClasspath(nameUri)) {
      throw new IllegalArgumentException(
          "unsupported schema " + nameUri.getScheme() + " only classpath: schema is supported.");
    }
    return urlClassPathResource(nameUri);
  }

  public static URL findNamedResource(String name) {
    URI nameUri = URI.create(name);
    if (isClasspath(nameUri)) {
      Iterable<URL> resources = urlClassPathResource(nameUri);
      return firstElementOrNull(resources);
    }
    try {
      return nameUri.toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static boolean isClasspath(URI nameUri) {
    return nameUri.getScheme().equalsIgnoreCase("classpath");
  }

  private static Iterable<URL> urlClassPathResource(URI uri) {
    ClassLoader cl =
        MoreObjects.firstNonNull(
            Thread.currentThread().getContextClassLoader(), ResourceLocator.class.getClassLoader());
    try {
      Enumeration<URL> enumeration = cl.getResources(uri.getSchemeSpecificPart());
      return asIterable(enumeration);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static URL firstElementOrNull(Iterable<URL> urls) {
    for (URL url : urls) {
      return url;
    }
    return null;
  }

  private static <T> Iterable<T> asIterable(Enumeration<T> enumeration) {
    return () ->
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            return enumeration.hasMoreElements();
          }

          @Override
          public T next() {
            return enumeration.nextElement();
          }
        };
  }
}
