package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketUtils.UnixDomainHttpEndpoint;
import org.junit.Test;

public class OkHttpUnixSocketUtilsTest {

  @Test
  public void splitOnlyWithFile() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock"));

    assertEquals("/some/path.sock", out.unixDomainFile.toString());
    assertEquals("/", out.pathSegment);
  }

  @Test
  public void splitOnlyWithFileAndEndpoint() {
    UnixDomainHttpEndpoint out =
        UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path.sock/hello"));

    assertEquals("/some/path.sock", out.unixDomainFile.toString());
    assertEquals("/hello", out.pathSegment);
  }

  @Test(expected = IllegalStateException.class)
  public void missingSockFile() {
    UnixDomainHttpEndpoint.parseFrom(URI.create("http+unix:///some/path/hello"));
  }
}
