package org.apache.flink.statefun.e2e.smoke;

import com.google.protobuf.Any;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public class Constants {

  public static final IngressIdentifier<Any> IN = new IngressIdentifier<>(Any.class, "", "source");

  public static final EgressIdentifier<Any> OUT = new EgressIdentifier<>("", "sink", Any.class);

  public static final FunctionType FN_TYPE = new FunctionType("v", "f1");

  public static final EgressIdentifier<Any> VERIFICATION_RESULT =
      new EgressIdentifier<>("", "verification", Any.class);
}
