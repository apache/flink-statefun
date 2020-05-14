package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Map;
import org.junit.Test;

public class HttpFunctionProviderTest {

  @Test
  public void splitOnlyWithFile() {
    Map.Entry<String, String> out =
        HttpFunctionProvider.splitFilePathAndEndpointForUDS(
            URI.create("http+unix:///some/path.sock"));

    assertEquals("/some/path.sock", out.getKey());
    assertEquals("/", out.getValue());
  }

  @Test
  public void splitOnlyWithFileAndEndpoint() {
    Map.Entry<String, String> out =
        HttpFunctionProvider.splitFilePathAndEndpointForUDS(
            URI.create("http+unix:///some/path.sock/hello"));

    assertEquals("/some/path.sock", out.getKey());
    assertEquals("/hello", out.getValue());
  }

  @Test(expected = IllegalStateException.class)
  public void missingSockFile() {
    HttpFunctionProvider.splitFilePathAndEndpointForUDS(URI.create("http+unix:///some/path/hello"));
  }
}
