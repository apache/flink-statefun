package org.apache.flink.statefun.e2e.smoke;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class MoreExecutors {

  static ExecutorService newCachedDaemonThreadPool() {
    return Executors.newCachedThreadPool(
        r -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          return t;
        });
  }
}
