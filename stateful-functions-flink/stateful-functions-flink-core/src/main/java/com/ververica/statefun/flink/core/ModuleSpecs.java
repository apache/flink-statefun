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

package com.ververica.statefun.flink.core;

import com.ververica.statefun.flink.core.ModuleSpecs.ModuleSpec;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class ModuleSpecs implements Iterable<ModuleSpec>, Serializable {

  private static final long serialVersionUID = 1L;
  private final List<ModuleSpec> specs;

  private ModuleSpecs(List<ModuleSpec> specs) {
    this.specs = Objects.requireNonNull(specs);
  }

  public static ModuleSpecs fromPath(String rootDirectory) throws IOException {
    Objects.requireNonNull(rootDirectory);

    List<ModuleSpec> loadableModules = discoverLoadableArtifacts(rootDirectory);
    return new ModuleSpecs(loadableModules);
  }

  public static ModuleSpecs fromCollection(ModuleSpec... moduleSpecs) {
    List<ModuleSpec> loadableModules = Arrays.asList(moduleSpecs);
    return new ModuleSpecs(loadableModules);
  }

  /** Scans the given directory and looks for a List of artifacts ( */
  private static List<ModuleSpec> discoverLoadableArtifacts(String rootDirectory)
      throws IOException {
    File parent = new File(rootDirectory);
    if (!parent.exists()) {
      throw new IllegalArgumentException(rootDirectory + " does not exists.");
    }
    if (!parent.isDirectory()) {
      throw new RuntimeException(rootDirectory + " is not a directory.");
    }
    List<ModuleSpec> loadableFunctions = new ArrayList<>();
    for (File subDirectory : nullToEmpty(parent.listFiles())) {
      if (subDirectory.isDirectory()) {
        ModuleSpec loadableFunction = findLoadableModuleArtifacts(subDirectory.getAbsoluteFile());
        loadableFunctions.add(loadableFunction);
      }
    }
    return loadableFunctions;
  }

  private static ModuleSpec findLoadableModuleArtifacts(File subDirectory) throws IOException {
    ModuleSpec.Builder builder = ModuleSpec.builder();

    for (File jarFile : nullToEmpty(subDirectory.listFiles())) {
      if (jarFile.isFile() && jarFile.getName().endsWith(".jar")) {
        builder.withFile(jarFile.getAbsoluteFile());
      }
    }
    return builder.build();
  }

  private static File[] nullToEmpty(File[] elements) {
    return elements == null ? new File[0] : elements;
  }

  public List<ModuleSpec> modules() {
    return specs;
  }

  @Override
  public Iterator<ModuleSpec> iterator() {
    return specs.iterator();
  }

  public static final class ModuleSpec implements Serializable {

    private static final long serialVersionUID = 1;
    private final List<URI> artifactUrls;

    private ModuleSpec(List<URI> artifacts) {
      this.artifactUrls = Collections.unmodifiableList(artifacts);
    }

    static Builder builder() {
      return new Builder();
    }

    public List<URI> artifactUris() {
      return artifactUrls;
    }

    static final class Builder {
      private final TreeSet<URI> artifacts = new TreeSet<>();

      Builder withFile(File file) throws IOException {
        Objects.requireNonNull(file);
        artifacts.add(file.getCanonicalFile().toURI());
        return this;
      }

      Builder withUri(URI uri) {
        Objects.requireNonNull(uri);
        artifacts.add(uri);
        return this;
      }

      ModuleSpec build() {
        List<URI> sortedCopy = new ArrayList<>(artifacts);
        return new ModuleSpec(sortedCopy);
      }
    }
  }
}
