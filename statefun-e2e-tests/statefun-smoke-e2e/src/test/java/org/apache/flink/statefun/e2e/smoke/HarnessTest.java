package org.apache.flink.statefun.e2e.smoke;

import static org.apache.flink.statefun.e2e.smoke.Utils.awaitVerificationSuccess;
import static org.apache.flink.statefun.e2e.smoke.Utils.startProtobufServer;

import com.google.protobuf.Any;
import org.apache.flink.statefun.flink.harness.Harness;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HarnessTest {

  private static final Logger LOG = LoggerFactory.getLogger(HarnessTest.class);

  @Ignore
  @Test(timeout = 1_000 * 60 * 2)
  public void miniClusterTest() throws Exception {
    Harness harness = new Harness();

    // set Flink related configuration.
    harness.withConfiguration(
        "classloader.parent-first-patterns.additional",
        "org.apache.flink.statefun;org.apache.kafka;com.google.protobuf");
    harness.withConfiguration("restart-strategy", "fixed-delay");
    harness.withConfiguration("restart-strategy.fixed-delay.attempts", "2147483647");
    harness.withConfiguration("restart-strategy.fixed-delay.delay", "1sec");
    harness.withConfiguration("execution.checkpointing.interval", "2sec");
    harness.withConfiguration("execution.checkpointing.mode", "EXACTLY_ONCE");
    harness.withConfiguration("execution.checkpointing.max-concurrent-checkpoints", "3");
    harness.withConfiguration("parallelism.default", "2");
    harness.withConfiguration("state.checkpoints.dir", "file:///tmp/checkpoints");

    // start the Protobuf server
    SimpleProtobufServer.StartedServer<Any> started = startProtobufServer();

    // configure test parameters.
    ModuleParameters parameters = new ModuleParameters();
    parameters.setMaxFailures(1);
    parameters.setMessageCount(100_000);
    parameters.setNumberOfFunctionInstances(128);
    parameters.setVerificationServerHost("localhost");
    parameters.setVerificationServerPort(started.port());
    parameters.asMap().forEach(harness::withGlobalConfiguration);

    // run the harness.
    try (AutoCloseable ignored = startHarnessInTheBackground(harness)) {
      awaitVerificationSuccess(started.results(), parameters.getNumberOfFunctionInstances());
    }

    LOG.info("All done.");
  }

  private static AutoCloseable startHarnessInTheBackground(Harness harness) {
    Thread t =
        new Thread(
            () -> {
              try {
                harness.start();
              } catch (InterruptedException ignored) {
                LOG.info("Harness Thread was interrupted. Exiting...");
              } catch (Exception exception) {
                LOG.info("Something happened while trying to run the Harness.", exception);
              }
            });
    t.setName("harness-runner");
    t.setDaemon(true);
    t.start();
    return t::interrupt;
  }
}
