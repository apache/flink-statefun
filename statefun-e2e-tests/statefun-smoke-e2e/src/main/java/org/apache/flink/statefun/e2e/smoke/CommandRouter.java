package org.apache.flink.statefun.e2e.smoke;

import com.google.protobuf.Any;
import java.util.Objects;
import org.apache.flink.statefun.e2e.smoke.generated.SourceCommand;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;

public class CommandRouter implements Router<Any> {
  private final Ids ids;

  public CommandRouter(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public void route(Any any, Downstream<Any> downstream) {
    SourceCommand sourceCommand = ProtobufUtils.unpack(any, SourceCommand.class);
    FunctionType type = Constants.FN_TYPE;
    String id = ids.idOf(sourceCommand.getTarget());
    downstream.forward(type, id, any);
  }
}
