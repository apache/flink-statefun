package org.apache.flink.statefun.flink.core.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.flink.metrics.Counter;
import org.junit.Test;

public class NonNegativeCounterTest {

  @Test
  public void testNonNegativeCounter() throws Exception {
    Counter counter = new NonNegativeCounter();

    counter.inc();
    assertThat(counter.getCount(), is(1L));

    counter.inc(2);
    assertThat(counter.getCount(), is(3L));

    counter.dec(4);
    assertThat(counter.getCount(), is(0L));

    counter.dec();
    assertThat(counter.getCount(), is(0L));
  }
}
