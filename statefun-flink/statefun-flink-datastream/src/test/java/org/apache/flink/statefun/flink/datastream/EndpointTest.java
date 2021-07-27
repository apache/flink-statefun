package org.apache.flink.statefun.flink.datastream;

import org.junit.Test;

/** Test validation of {@link Endpoint} specification. */
public class EndpointTest {

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardNamespaceValidation() {
    Endpoint.http("*/name", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardNameValidation() {
    Endpoint.http("namespace/na*e", "");
  }

  @Test
  public void testValidFunctions() {
    Endpoint.http("namespace/name", "https://endpoint");
    Endpoint.http("namespace/*", "https://endpoint/{function.name}");
  }
}
