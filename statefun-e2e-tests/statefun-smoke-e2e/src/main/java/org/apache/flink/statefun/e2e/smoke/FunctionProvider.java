package org.apache.flink.statefun.e2e.smoke;

import java.util.Objects;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

public class FunctionProvider implements StatefulFunctionProvider {
  private final Ids ids;

  public FunctionProvider(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    CommandInterpreter interpreter = new CommandInterpreter(ids);
    return new Fn(interpreter);
  }
}
