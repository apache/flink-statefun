package org.apache.flink.statefun.flink.datastream;

import org.junit.Test;

/** Test validation of {@link Endpoint} specification. */
public class EndpointTest {

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardNamespaceValidation() {
    Endpoint.withSpec("*/name", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardNameValidation() {
    Endpoint.withSpec("namespace/na*e", "");
  }

  @Test
  public void testValidFunctions() {
    Endpoint.withSpec("namespace/name", "");
    Endpoint.withSpec("namespace/*", "");
  }
}
