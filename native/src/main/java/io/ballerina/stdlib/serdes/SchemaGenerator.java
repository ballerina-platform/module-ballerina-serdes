/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufFileBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.FAILED_WRITE_FILE;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.PROTO3;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_GENERATION_FAILURE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Generates a Protobuf schema for a given data type.
 */
public class SchemaGenerator {

    /**
     * Creates a schema for a given data type and adds to native data.
     *
     * @param serdes    Serializer or Deserializer object.
     * @param bTypedesc Data type that is being serialized.
     * @return {@code BError}, if there are schema generation errors, null otherwise.
     */
    @SuppressWarnings("unused")
    public static Object generateSchema(BObject serdes, BTypedesc bTypedesc) {
        try {
            ProtobufFileBuilder protobufFile = new ProtobufFileBuilder();
            ProtobufMessageBuilder protobufMessageBuilder = buildProtobufMessageFromBallerinaTypedesc(
                    bTypedesc.getDescribingType());
            Descriptor messageDescriptor = protobufFile.addMessageType(protobufMessageBuilder).build();
            serdes.addNativeData(SCHEMA_NAME, messageDescriptor);
            serdes.addNativeData(PROTO3, protobufFile.toString());
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (DescriptorValidationException e) {
            String errorMessage = SCHEMA_GENERATION_FAILURE + e.getMessage();
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return null;
    }

    @SuppressWarnings("unused")
    public static Object generateProtoFile(BObject serdes, BString filePath) {
        String filePathName = filePath.getValue();
        try (FileWriter file = new FileWriter(filePathName, StandardCharsets.UTF_8)) {
            String proto3 = (String) serdes.getNativeData(PROTO3);
            file.write(proto3);
        } catch (IOException e) {
            String errorMessage = FAILED_WRITE_FILE + e.getMessage();
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return null;
    }

    private static ProtobufMessageBuilder buildProtobufMessageFromBallerinaTypedesc(Type ballerinaType) {
        ProtobufMessageBuilder messageBuilder;
        String messageName;

        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                messageName = Utils.createMessageName(referredType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveType(messageBuilder, referredType);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                messageName = DECIMAL_VALUE;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveDecimal(messageBuilder);
                break;
            }

            case TypeTags.UNION_TAG: {
                messageName = UNION_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            case TypeTags.ARRAY_TAG: {
                messageName = ARRAY_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            case TypeTags.RECORD_TYPE_TAG: {
                messageName = referredType.getName();
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            case TypeTags.MAP_TAG: {
                messageName = MAP_BUILDER;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            case TypeTags.TABLE_TAG: {
                messageName = TABLE_BUILDER;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            case TypeTags.TUPLE_TAG: {
                messageName = TUPLE_BUILDER;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                return new BallerinaStructuredTypeMessageGenerator(referredType,
                        messageBuilder).generateMessageDefinition();
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(ProtobufMessageBuilder messageBuilder,
                                                                  Type ballerinaType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType,
                Constants.ATOMIC_FIELD_NAME, 1);
        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(ProtobufMessageBuilder messageBuilder) {
        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, 1);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                2);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, 3);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }
}
