package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import org.junit.Test;

public class UnixDomainHttpEndpointTest {

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

  @Test
  public void validateUdsEndpoint() {
    assertFalse(UnixDomainHttpEndpoint.validate(URI.create("http:///bar.foo.com/some/path")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseNonUdsEndpoint() {
    UnixDomainHttpEndpoint.parseFrom(URI.create("http:///bar.foo.com/some/path"));
  }
}
