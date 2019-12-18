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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.Charsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ResourceLocatorTest {

  @Parameterized.Parameters
  public static Collection<Configuration> filesystemTypes() {
    return Arrays.asList(Configuration.unix(), Configuration.osX(), Configuration.windows());
  }

  private final FileSystem fileSystem;

  public ResourceLocatorTest(Configuration filesystemConfiguration) {
    this.fileSystem = Jimfs.newFileSystem(filesystemConfiguration);
  }

  @Test
  public void classPathExample() throws IOException {
    final Path firstModuleDir = createDirectoryWithAFile("first", "module.yaml");
    final Path secondModuleDir = createDirectoryWithAFile("second", "module.yaml");

    ClassLoader urlClassLoader = urlClassLoader(firstModuleDir, secondModuleDir);

    try (SetContextClassLoader ignored = new SetContextClassLoader(urlClassLoader)) {

      Iterable<URL> foundUrls = ResourceLocator.findNamedResources("classpath:module.yaml");

      assertThat(
          foundUrls,
          contains(
              url(firstModuleDir.resolve("module.yaml")),
              url(secondModuleDir.resolve("module.yaml"))));
    }
  }

  @Test
  public void classPathSingleResourceExample() {
    URL url = ResourceLocator.findNamedResource("classpath:test-descriptors.bin");

    assertThat(url, notNullValue());
  }

  @Test
  public void absolutePathExample() throws IOException {
    Path modulePath = createDirectoryWithAFile("some-module", "module.yaml").resolve("module.yaml");

    URL url = ResourceLocator.findNamedResource(modulePath.toUri().toString());

    assertThat(url, is(url(modulePath)));
  }

  private Path createDirectoryWithAFile(
      String basedir, @SuppressWarnings("SameParameterValue") String filename) throws IOException {
    final Path dir = fileSystem.getPath(basedir);
    Files.createDirectories(dir);

    Path file = dir.resolve(filename);
    Files.write(file, "hello world".getBytes(Charsets.UTF_8));

    return dir;
  }

  private static ClassLoader urlClassLoader(Path... urlPath) {
    URL[] urls = Arrays.stream(urlPath).map(ResourceLocatorTest::url).toArray(URL[]::new);
    return new URLClassLoader(urls);
  }

  private static URL url(Path path) {
    try {
      return path.toUri().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
}
