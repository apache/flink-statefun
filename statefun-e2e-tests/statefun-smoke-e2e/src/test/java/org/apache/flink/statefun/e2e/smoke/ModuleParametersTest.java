package org.apache.flink.statefun.e2e.smoke;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ModuleParametersTest {

  @Test
  public void exampleUsage() {
    Map<String, String> keys = Collections.singletonMap("messageCount", "1");
    ModuleParameters parameters = ModuleParameters.from(keys);

    assertThat(parameters.getMessageCount(), is(1));
  }

  @Test
  public void roundTrip() {
    ModuleParameters original = new ModuleParameters();
    original.setCommandDepth(1234);

    ModuleParameters deserialized = ModuleParameters.from(original.asMap());

    assertThat(deserialized.getCommandDepth(), is(1234));
  }
}
