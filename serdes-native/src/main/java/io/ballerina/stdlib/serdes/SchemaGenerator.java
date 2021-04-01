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

import com.google.protobuf.Descriptors;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;

import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Generates a Protobuf schema for a given data type.
 */
public class SchemaGenerator {

    static final String SCHEMA_NAME = "schema";
    static final String SCHEMA_BUILDER_NAME = "Schema.proto";
    static final String REPEATED_LABEL = "repeated";
    static final String OPTIONAL_LABEL = "optional";

    static final String ATOMIC_FIELD_NAME = "atomicField";
    static final String ARRAY_FIELD_NAME = "arrayfield";
    static final String ARRAY_BUILDER_NAME = "ArrayBuilder";
    static final String UNION_BUILDER_NAME = "UnionBuilder";
    static final String UNION_FIELD_NAME = "UnionField";
    static final String NULL_FIELD_NAME = "nullField";

    static final String NESTED_UNION_FIELD_NAME = "unionelement";
    static final String UNION_TYPE_IDENTIFIER = "ballerinauniontype";
    static final String UNION_FIELD_SEPARATOR = "__";

    static final String BYTES = "bytes";

    static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
    static final String SCHEMA_GENERATION_FAILURE = "Failed to generate schema: ";

    /**
     * Creates a schema for a given data type and adds to native data.
     *
     * @param serdes  Serializer or Deserializer object.
     * @param typedesc Data type that is being serialized.
     * @return {@code BError}, if there are schema generation errors, null otherwise.
     */
    public static Object generateSchema(BObject serdes, BTypedesc typedesc) {
        ProtobufMessage protobufMessage;

        try {
            protobufMessage = buildProtobufMessageFromTypedesc(typedesc);
        } catch (BError e) {
            return e;
        }

        ProtobufSchemaBuilder schemaBuilder = ProtobufSchemaBuilder.newSchemaBuilder(SCHEMA_BUILDER_NAME);
        schemaBuilder.addMessageToProtoSchema(protobufMessage);

        Descriptors.Descriptor schema = null;
        try {
            schema = schemaBuilder.build();
        } catch (Descriptors.DescriptorValidationException e) {
            return createSerdesError(SCHEMA_GENERATION_FAILURE + e.getMessage(), SERDES_ERROR);
        }
        serdes.addNativeData(SCHEMA_NAME, schema);

        return null;
    }

    private static ProtobufMessage buildProtobufMessageFromTypedesc(BTypedesc typedesc) {
        Type type = typedesc.getDescribingType();

        if (type.getTag() <= TypeTags.BOOLEAN_TAG) {
            String ballerinaToProtoMap = DataTypeMapper.getProtoTypeFromTag(type.getTag());

            ProtobufMessageBuilder messageBuilder = ProtobufMessage.newMessageBuilder(ballerinaToProtoMap);
            buildProtobufMessageForPrimitive(messageBuilder, ballerinaToProtoMap, ATOMIC_FIELD_NAME, 1);

            return messageBuilder.build();
        } else if (type.getTag() == TypeTags.UNION_TAG) {
            ProtobufMessage protobufMessage = buildProtobufMessageForUnion(type, UNION_FIELD_NAME);
            ProtobufMessageBuilder messageBuilder = ProtobufMessage.newMessageBuilder(UNION_BUILDER_NAME);

            messageBuilder.addNestedMessage(protobufMessage);
            messageBuilder.addField(OPTIONAL_LABEL, UNION_FIELD_NAME, ATOMIC_FIELD_NAME, 1);

            return messageBuilder.build();
        } else if (type.getTag() == TypeTags.ARRAY_TAG) {
            ArrayType arrayType = (ArrayType) type;

            ProtobufMessageBuilder messageBuilder = ProtobufMessage.newMessageBuilder(ARRAY_BUILDER_NAME);
            buildProtobufMessageForArray(messageBuilder, arrayType, ARRAY_FIELD_NAME, 1, 1);

            return messageBuilder.build();
        } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            RecordType recordType = (RecordType) type;

            return buildProtobufMessageForRecord(recordType.getFields(), type.getName());
        } else {
            throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }

    }

    private static void buildProtobufMessageForPrimitive(ProtobufMessageBuilder messageBuilder, String type,
                                                         String name, int number) {
        messageBuilder.addField(OPTIONAL_LABEL, type, name, number);
    }

    private static void buildProtobufMessageForArray(ProtobufMessageBuilder messageBuilder, ArrayType arrayType,
                                                     String name, int number, int unionFieldIdentifier) {
        Type type = arrayType.getElementType();

        if (type.getTag() == TypeTags.UNION_TAG) {
            String fieldName = name + "_" + UNION_TYPE_IDENTIFIER;
            messageBuilder.addNestedMessage(buildProtobufMessageForUnion(type, fieldName));
            messageBuilder.addField(REPEATED_LABEL, fieldName, name, number);
        } else if (type.getTag() <= TypeTags.BOOLEAN_TAG) {
            String protoElementType = DataTypeMapper.getProtoTypeFromTag(type.getTag());

            if (protoElementType.equals(BYTES)) {
                messageBuilder.addField(OPTIONAL_LABEL, protoElementType, name, number);
            } else {
                messageBuilder.addField(REPEATED_LABEL, protoElementType, name, number);
            }
        } else if (type.getTag() == TypeTags.ARRAY_TAG) {
            ArrayType nestedArrayType = (ArrayType) type;
            String nestedMessageName;

            int elementTag = nestedArrayType.getElementType().getTag();
            if (elementTag == TypeTags.UNION_TAG || elementTag == TypeTags.ARRAY_TAG) {
                nestedMessageName = NESTED_UNION_FIELD_NAME + unionFieldIdentifier;
                unionFieldIdentifier++;
            } else {
                nestedMessageName = nestedArrayType.getElementType().getName();
            }

            ProtobufMessageBuilder nestedMessageBuilder = ProtobufMessage.newMessageBuilder(nestedMessageName);
            buildProtobufMessageForArray(
                    nestedMessageBuilder,
                    nestedArrayType,
                    nestedMessageName,
                    1,
                    unionFieldIdentifier
            );

            messageBuilder.addNestedMessage(nestedMessageBuilder.build());
            messageBuilder.addField(REPEATED_LABEL, nestedMessageName, name, number);

        } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            RecordType recordType = (RecordType) type;

            String[] elementNameHolder = type.getName().split(":");
            String elementType = elementNameHolder[elementNameHolder.length - 1];

            messageBuilder.addNestedMessage(
                    buildProtobufMessageForRecord(recordType.getFields(), recordType.getName())
            );
            messageBuilder.addField(REPEATED_LABEL, elementType, name, number);
        } else {
            throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }

        return;
    }

    private static ProtobufMessage buildProtobufMessageForRecord(Map<String, Field> dataTypeMap, String name) {
        ProtobufMessageBuilder messageBuilder = ProtobufMessage.newMessageBuilder(name);

        int number = 1;
        for (Map.Entry<String, Field> entry : dataTypeMap.entrySet()) {
            Type fieldType = entry.getValue().getFieldType();
            String fieldName = entry.getValue().getFieldName();

            if (fieldType.getTag() == TypeTags.UNION_TAG) {
                String unionFieldType = fieldName + "_" + UNION_TYPE_IDENTIFIER;
                ProtobufMessage nestedMessage = buildProtobufMessageForUnion(fieldType, unionFieldType);

                messageBuilder.addNestedMessage(nestedMessage);
                messageBuilder.addField(OPTIONAL_LABEL, unionFieldType, fieldName, number);
            } else if (fieldType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                RecordType recordType = (RecordType) fieldType;

                ProtobufMessage nestedMessage = buildProtobufMessageForRecord(
                        recordType.getFields(),
                        recordType.getName()
                );
                String nestedFieldType = recordType.getName();

                messageBuilder.addNestedMessage(nestedMessage);
                messageBuilder.addField(OPTIONAL_LABEL, nestedFieldType, fieldName, number);

            } else if (fieldType.getTag() == TypeTags.ARRAY_TAG) {
                ArrayType arrayType = (ArrayType) fieldType;

                buildProtobufMessageForArray(messageBuilder, arrayType, fieldName, number, 1);
            } else if (fieldType.getTag() <= TypeTags.BOOLEAN_TAG) {
                String protoFieldType = DataTypeMapper.getProtoTypeFromTag(fieldType.getTag());

                buildProtobufMessageForPrimitive(messageBuilder, protoFieldType, fieldName, number);
            } else {
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + fieldType.getName(), SERDES_ERROR);
            }
            number++;
        }

        return messageBuilder.build();
    }

    private static ProtobufMessage buildProtobufMessageForUnion(Type type, String name) {
        UnionType unionType = (UnionType) type;

        ProtobufMessageBuilder messageBuilder = ProtobufMessage.newMessageBuilder(name);

        int number = 1;
        for (Type memberType : unionType.getMemberTypes()) {
            if (memberType.getTag() <= TypeTags.BOOLEAN_TAG) {
                String ballerinaToProtoMap = DataTypeMapper.getProtoTypeFromTag(memberType.getTag());
                String fieldName = ballerinaToProtoMap + UNION_FIELD_SEPARATOR + name;

                buildProtobufMessageForPrimitive(messageBuilder, ballerinaToProtoMap, fieldName, number);
                number++;
            } else if (memberType.getTag() == TypeTags.NULL_TAG) {
                messageBuilder.addField(OPTIONAL_LABEL, "bool", NULL_FIELD_NAME, number);
                number++;
            } else if (memberType.getTag() == TypeTags.ARRAY_TAG) {
                ArrayType arrayType = (ArrayType) memberType;
                String protoType = DataTypeMapper.getProtoTypeFromTag(arrayType.getElementType().getTag());
                if (protoType == null) {
                    protoType = arrayType.getElementType().getName();
                }
                String fieldName = protoType + UNION_FIELD_SEPARATOR + "array_" + name;

                buildProtobufMessageForArray(messageBuilder, arrayType, fieldName, number, 1);
                number++;
            } else if (memberType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                RecordType recordType = (RecordType) memberType;
                String fieldName = memberType.getName() + UNION_FIELD_SEPARATOR + name;

                String[] elementNameHolder = recordType.getName().split(":");
                String elementType = elementNameHolder[elementNameHolder.length - 1] + "record";

                messageBuilder.addNestedMessage(buildProtobufMessageForRecord(recordType.getFields(),
                        recordType.getName() + "record"));
                messageBuilder.addField(OPTIONAL_LABEL, elementType, fieldName, number);
                number++;
            } else {
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
            }
        }

        return messageBuilder.build();
    }
}
