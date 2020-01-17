/*
 * Copyright 2019 Ververica GmbH.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.statefun.state.processor.union;

import com.ververica.statefun.sdk.Address;
import com.ververica.statefun.sdk.FunctionType;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.junit.Ignore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaggedBootstrapDataSerializerTest extends SerializerTestBase<TaggedBootstrapData> {

    private static final List<TypeSerializer<?>> TEST_PAYLOAD_SERIALIZERS = Arrays.asList(IntSerializer.INSTANCE, BooleanSerializer.INSTANCE);
    private static final Map<Class<?>, Integer> TYPE_TO_UNION_INDEX = new HashMap<>(2);
    static {
        TYPE_TO_UNION_INDEX.put(Integer.class, 0);
        TYPE_TO_UNION_INDEX.put(Boolean.class, 1);
    }

    public TaggedBootstrapDataSerializerTest() {
        super(
                new DeeplyEqualsChecker().withCustomCheck(
                        (o1, o2) -> o1 instanceof TaggedBootstrapData && o2 instanceof TaggedBootstrapData,
                        (o1, o2, checker) -> {
                            TaggedBootstrapData obj1 = (TaggedBootstrapData) o1;
                            TaggedBootstrapData obj2 = (TaggedBootstrapData) o2;
                            return obj1.getTarget().equals(obj2.getTarget())
                                    && obj1.getUnionIndex() == obj2.getUnionIndex()
                                    // equality checks on payload makes sense here since
                                    // the payloads are only booleans or integers in this test
                                    && obj1.getPayload().equals(obj2.getPayload());
                        }));
    }

    @Override
    protected TaggedBootstrapData[] getTestData() {
        final TaggedBootstrapData[] testData = new TaggedBootstrapData[3];
        testData[0] = integerPayloadBootstrapData(
                "test-namespace",
                "test-name",
                "test-id-1",
                1991);
        testData[1] = booleanPayloadBootstrapData(
                "test-namespace",
                "test-name-2",
                "test-id-80",
                false);
        testData[2] = integerPayloadBootstrapData(
                "test-namespace",
                "test-name",
                "test-id-56",
                1108);

        return testData;
    }

    private TaggedBootstrapData integerPayloadBootstrapData(
            String functionNamespace,
            String functionName,
            String functionId,
            int payload) {
        return new TaggedBootstrapData(
                addressOf(functionNamespace, functionName, functionId),
                payload,
                TYPE_TO_UNION_INDEX.get(Integer.class));
    }

    private TaggedBootstrapData booleanPayloadBootstrapData(
            String functionNamespace,
            String functionName,
            String functionId,
            boolean payload) {
        return new TaggedBootstrapData(
                addressOf(functionNamespace, functionName, functionId),
                payload,
                TYPE_TO_UNION_INDEX.get(Boolean.class));
    }

    @Override
    protected TypeSerializer<TaggedBootstrapData> createSerializer() {
        return new TaggedBootstrapDataSerializer(TEST_PAYLOAD_SERIALIZERS);
    }

    @Override
    protected Class<TaggedBootstrapData> getTypeClass() {
        return TaggedBootstrapData.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    // -----------------------------------------------------------------------------
    //  Ignored tests
    // -----------------------------------------------------------------------------

    @Override
    @Ignore
    public void testConfigSnapshotInstantiation() {
        // test ignored; this is a test that is only relevant for serializers that are used for persistent data
    }

    @Override
    @Ignore
    public void testSnapshotConfigurationAndReconfigure() {
        // test ignored; this is a test that is only relevant for serializers that are used for persistent data
    }

    // -----------------------------------------------------------------------------
    //  Utilities
    // -----------------------------------------------------------------------------

    private static Address addressOf(String functionNamespace, String functionName, String functionId) {
        return new Address(
                new FunctionType(functionNamespace, functionName),
                functionId);
    }
}
