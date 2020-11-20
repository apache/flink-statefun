package org.apache.flink.statefun.e2e.smoke;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.flink.statefun.e2e.common.StatefulFunctionsAppContainers;
import org.apache.flink.statefun.e2e.smoke.generated.VerificationResult;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;

public final class SmokeRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SmokeRunner.class);

  public static void run(ModuleParameters parameters) throws Throwable {
    SimpleProtobufServer.StartedServer<VerificationResult> server = startProtobufServer();
    parameters.setVerificationServerHost("host.testcontainers.internal");
    parameters.setVerificationServerPort(server.port());

    StatefulFunctionsAppContainers.Builder builder =
        StatefulFunctionsAppContainers.builder("smoke", 2);
    builder.exposeMasterLogs(LOG);

    // set the test module parameters as global configurations, so that
    // it can be deserialized at Module#configure()
    parameters.asMap().forEach(builder::withModuleGlobalConfiguration);

    // run the test
    Testcontainers.exposeHostPorts(server.port());
    StatefulFunctionsAppContainers app = builder.build();

    run(
        app,
        () -> {
          Supplier<VerificationResult> results = server.results();
          Set<Integer> successfullyVerified = new HashSet<>();
          while (successfullyVerified.size() != parameters.getNumberOfFunctionInstances()) {
            VerificationResult result = results.get();
            if (result.getActual() == result.getExpected()) {
              successfullyVerified.add(result.getId());
            }
          }
        });
  }

  private static SimpleProtobufServer.StartedServer<VerificationResult> startProtobufServer() {
    SimpleProtobufServer<VerificationResult> server =
        new SimpleProtobufServer<>(VerificationResult.parser());
    return server.start();
  }

  private static void run(StatefulFunctionsAppContainers app, ThrowingRunnable<Throwable> r)
      throws Throwable {
    Statement statement =
        app.apply(
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                r.run();
              }
            },
            Description.EMPTY);

    statement.evaluate();
  }
}
