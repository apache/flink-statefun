package org.apache.flink.statefun.e2e.smoke;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.junit.Test;

public class CommandGeneratorTest {

  @Test
  public void usageExample() {
    ModuleParameters parameters = new ModuleParameters();
    CommandGenerator generator = new CommandGenerator(new JDKRandomGenerator(), parameters);

    SourceCommand command = generator.get();

    assertThat(command.getTarget(), notNullValue());
    assertThat(command.getCommands(), notNullValue());
  }
}
