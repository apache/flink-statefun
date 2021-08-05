package org.apache.flink.statefun.flink.io.kinesis.binders;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.net.URL;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.extensions.ExtensionResolver;
import org.apache.flink.statefun.flink.io.kafka.binders.AutoRoutableKafkaIngressComponentBinderV1Test;
import org.apache.flink.statefun.flink.io.testutils.TestModuleBinder;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.kinesis.egress.KinesisEgressSpec;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.Test;

public class GenericKinesisEgressComponentBinderV1Test {
  private static final ObjectMapper OBJ_MAPPER = new ObjectMapper(new YAMLFactory());

  private static final String SPEC_YAML_PATH = "kinesis-io-binders/generic-kinesis-egress-v1.yaml";

  @Test
  public void exampleUsage() throws Exception {
    final ComponentJsonObject component = testComponent();
    final TestModuleBinder testModuleBinder = new TestModuleBinder();

    GenericKinesisEgressComponentBinderV1.INSTANCE.bind(
        component, testModuleBinder, new TestExtensionResolver());

    final EgressIdentifier<TypedValue> expectedEgressId =
        new EgressIdentifier<>("com.foo.bar", "test-egress", TypedValue.class);
    assertThat(testModuleBinder.getEgress(expectedEgressId), instanceOf(KinesisEgressSpec.class));
  }

  private static class TestExtensionResolver implements ExtensionResolver {
    @Override
    public <T> T resolveExtension(TypeName typeName, Class<T> extensionClass) {
      throw new UnsupportedOperationException();
    }
  }

  private static ComponentJsonObject testComponent() throws Exception {
    return new ComponentJsonObject(
        GenericKinesisEgressComponentBinderV1.KIND_TYPE, loadComponentSpec(SPEC_YAML_PATH));
  }

  private static JsonNode loadComponentSpec(String yamlPath) throws Exception {
    final URL url =
        AutoRoutableKafkaIngressComponentBinderV1Test.class.getClassLoader().getResource(yamlPath);
    return OBJ_MAPPER.readTree(url);
  }
}
