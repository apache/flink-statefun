package org.apache.flink.statefun.flink.core.httpfn;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.sun.net.httpserver.HttpServer;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.Test;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import org.newsclub.net.unix.AFUNIXSocketFactory;

import javax.net.ServerSocketFactory;

public class HttpFunctionProviderTest {

  @Test
  public void splitOnlyWithFile() {
    Map.Entry<String, String> out =
        HttpFunctionProvider.splitFilePathAndEndpointForUDS(
            URI.create("http+unix:///some/path.sock"));

    assertEquals("/some/path.sock", out.getKey());
    assertEquals("/", out.getValue());
  }

  @Test
  public void splitOnlyWithFileAndEndpoint() {
    Map.Entry<String, String> out =
        HttpFunctionProvider.splitFilePathAndEndpointForUDS(
            URI.create("http+unix:///some/path.sock/hello"));

    assertEquals("/some/path.sock", out.getKey());
    assertEquals("/hello", out.getValue());
  }

  @Test(expected = IllegalStateException.class)
  public void missingSockFile() {
    HttpFunctionProvider.splitFilePathAndEndpointForUDS(URI.create("http+unix:///some/path/hello"));
  }

  @Test
  public void testBuildUDSClient() throws IOException, ExecutionException, InterruptedException {
    OkHttpClient sharedClient = OkHttpUtils.newClient();
    String sockFile = "/tmp/" + UUID.randomUUID().toString() + ".sock";
    URI path = URI.create("http+unix://" + sockFile);

    FromFunction expected = FromFunction.newBuilder().build();
    try (MockWebServer server = new MockWebServer()) {
      // Force the mock web server to use my server socket factory
      server.setServerSocketFactory(udsSocketFactory(sockFile));
      server.enqueue(new MockResponse().setBody(new Buffer().write(expected.toByteArray())));
      server.start();

      HttpFunctionSpec spec = new HttpFunctionSpec(
              new FunctionType("my-namespace", "my-type"),
              path,
              Collections.emptyList(),
              Duration.ofSeconds(5),
              1
      );

      RequestReplyClient requestReplyClient = HttpFunctionProvider.buildHttpClient(sharedClient, spec);
      FromFunction result = requestReplyClient.call(ToFunction.newBuilder().build()).get();

      assertEquals(expected, result);
    }
  }

  private ServerSocketFactory udsSocketFactory(String sockFile) {
    return new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket() throws IOException {
        return AFUNIXServerSocket.forceBindOn(new AFUNIXSocketAddress(new File(sockFile)));
      }

      @Override
      public ServerSocket createServerSocket(int i) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1) throws IOException {
        return createServerSocket();
      }

      @Override
      public ServerSocket createServerSocket(int i, int i1, InetAddress inetAddress) throws IOException {
        return createServerSocket();
      }
    };
  }
}
