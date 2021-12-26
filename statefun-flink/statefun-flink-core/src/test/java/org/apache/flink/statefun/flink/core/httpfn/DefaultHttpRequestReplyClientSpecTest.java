package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.Assert.*;

import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

public class DefaultHttpRequestReplyClientSpecTest {

  @Test
  public void jsonSerDe() throws JsonProcessingException {
    final Duration callTimeout = Duration.ofDays(1L);
    final Duration connectTimeout = Duration.ofNanos(2L);
    final Duration readTimeout = Duration.ofSeconds(3L);
    final Duration writeTimeout = Duration.ofMillis(4L);

    final DefaultHttpRequestReplyClientSpec.Timeouts timeouts =
        new DefaultHttpRequestReplyClientSpec.Timeouts();
    timeouts.setCallTimeout(callTimeout);
    timeouts.setConnectTimeout(connectTimeout);
    timeouts.setReadTimeout(readTimeout);
    timeouts.setWriteTimeout(writeTimeout);

    final DefaultHttpRequestReplyClientSpec defaultHttpRequestReplyClientSpec =
        new DefaultHttpRequestReplyClientSpec();
    defaultHttpRequestReplyClientSpec.setTimeouts(timeouts);

    final ObjectMapper objectMapper = StateFunObjectMapper.create();
    final ObjectNode json = defaultHttpRequestReplyClientSpec.toJson(objectMapper);

    final DefaultHttpRequestReplyClientSpec deserializedHttpRequestReplyClientSpec =
        DefaultHttpRequestReplyClientSpec.fromJson(objectMapper, json);

    assertThat(deserializedHttpRequestReplyClientSpec.getTimeouts(), equalTimeouts(timeouts));
  }

  private static TypeSafeDiagnosingMatcher<DefaultHttpRequestReplyClientSpec.Timeouts>
      equalTimeouts(DefaultHttpRequestReplyClientSpec.Timeouts timeouts) {
    return new TimeoutsEqualityMatcher(timeouts);
  }

  private static class TimeoutsEqualityMatcher
      extends TypeSafeDiagnosingMatcher<DefaultHttpRequestReplyClientSpec.Timeouts> {
    private final DefaultHttpRequestReplyClientSpec.Timeouts expected;

    private TimeoutsEqualityMatcher(DefaultHttpRequestReplyClientSpec.Timeouts timeouts) {
      this.expected = timeouts;
    }

    @Override
    protected boolean matchesSafely(
        DefaultHttpRequestReplyClientSpec.Timeouts timeouts, Description description) {
      boolean matching = true;

      if (!timeouts.getCallTimeout().equals(expected.getCallTimeout())) {
        description
            .appendText("expected ")
            .appendValue(expected.getCallTimeout())
            .appendText(" found ")
            .appendValue(timeouts.getCallTimeout());
        matching = false;
      }

      if (!timeouts.getReadTimeout().equals(expected.getReadTimeout())) {
        description
            .appendText("expected ")
            .appendValue(expected.getReadTimeout())
            .appendText(" found ")
            .appendValue(timeouts.getReadTimeout());
        matching = false;
      }

      if (!timeouts.getWriteTimeout().equals(expected.getWriteTimeout())) {
        description
            .appendText("expected ")
            .appendValue(expected.getWriteTimeout())
            .appendText(" found ")
            .appendValue(timeouts.getWriteTimeout());
        matching = false;
      }

      if (!timeouts.getConnectTimeout().equals(expected.getConnectTimeout())) {
        description
            .appendText("expected ")
            .appendValue(expected.getConnectTimeout())
            .appendText(" found ")
            .appendValue(timeouts.getConnectTimeout());
        matching = false;
      }

      return matching;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Matches equality of Timeouts");
    }
  }
}
