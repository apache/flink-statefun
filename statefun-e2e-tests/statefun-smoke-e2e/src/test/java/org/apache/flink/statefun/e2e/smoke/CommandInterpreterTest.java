package org.apache.flink.statefun.e2e.smoke;

import static org.apache.flink.statefun.e2e.smoke.Utils.aStateModificationCommand;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.protobuf.Any;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.junit.Test;

public class CommandInterpreterTest {

  @Test
  public void exampleUsage() {
    CommandInterpreter interpreter = new CommandInterpreter(new Ids(10));

    PersistedValue<Long> state = PersistedValue.of("state", Long.class);
    Context context = new MockContext();
    SourceCommand sourceCommand = aStateModificationCommand();

    interpreter.interpret(state, context, Any.pack(sourceCommand));

    assertThat(state.get(), is(1L));
  }

  private static final class MockContext implements Context {

    @Override
    public Address self() {
      return null;
    }

    @Override
    public Address caller() {
      return null;
    }

    @Override
    public void send(Address address, Object o) {}

    @Override
    public <T> void send(EgressIdentifier<T> egressIdentifier, T t) {}

    @Override
    public void sendAfter(Duration duration, Address address, Object o) {}

    @Override
    public <M, T> void registerAsyncOperation(M m, CompletableFuture<T> completableFuture) {}
  }
}
