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
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.FAILED_WRITE_FILE;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.PROTO3;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_GENERATION_FAILURE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
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
            ProtobufMessageBuilder protobufMessageBuilder =
                    buildProtobufMessageFromBallerinaTypedesc(bTypedesc.getDescribingType());
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

        switch (ballerinaType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                int fieldNumber = 1;
                messageName = Utils.createMessageName(ballerinaType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveType(messageBuilder, ballerinaType, ATOMIC_FIELD_NAME,
                        fieldNumber);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                messageName = Utils.createMessageName(ballerinaType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveDecimal(messageBuilder);
                break;
            }

            case TypeTags.UNION_TAG: {
                messageName = UNION_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForUnionType(messageBuilder, (UnionType) ballerinaType);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                int fieldNumber = 1;
                ArrayType arrayType = (ArrayType) ballerinaType;
                messageName = ARRAY_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);

                generateMessageDefinitionForArrayType(messageBuilder, arrayType, ARRAY_FIELD_NAME, fieldNumber);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) ballerinaType;
                messageName = recordType.getName();
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForRecordType(messageBuilder, recordType);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + ballerinaType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(ProtobufMessageBuilder messageBuilder,
                                                                  Type ballerinaType, String fieldName,
                                                                  int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        ProtobufMessageFieldBuilder messageField =
                new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName, fieldNumber);
        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(ProtobufMessageBuilder messageBuilder) {
        int fieldNumber = 1;

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField =
                new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE, fieldNumber++);
        ProtobufMessageFieldBuilder precisionField =
                new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION, fieldNumber++);
        ProtobufMessageFieldBuilder valueField =
                new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE, fieldNumber);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }

    private static Map.Entry<String, Type> mapUnionMemberToMapEntry(Type type) {
        String typeName = type.getName();
        if (type.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getDimensions((ArrayType) type);
            typeName = Utils.getElementTypeOfBallerinaArray((ArrayType) type);

            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention
                            + TYPE_SEPARATOR + UNION_FIELD_NAME;

            return Map.entry(key, type);
        }

        if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            String key = type.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, type);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
    }

    private static void generateMessageDefinitionForUnionType(ProtobufMessageBuilder messageBuilder,
                                                              UnionType unionType) {
        int fieldNumber = 1;

        List<Type> memberTypes =
                unionType.getMemberTypes().stream().map(SchemaGenerator::mapUnionMemberToMapEntry).sorted(
                        Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());

        // Member field names are prefixed with ballerina type name to avoid name collision in proto message definition
        for (Type memberType : memberTypes) {
            String fieldName;

            switch (memberType.getTag()) {
                case TypeTags.NULL_TAG: {
                    fieldName = NULL_FIELD_NAME;
                    ProtobufMessageFieldBuilder nilField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BOOL, fieldName, fieldNumber);
                    messageBuilder.addField(nilField);
                    break;
                }

                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    fieldName = memberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    generateMessageDefinitionForPrimitiveType(messageBuilder, memberType, fieldName, fieldNumber);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(DECIMAL_VALUE);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    fieldName = memberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(memberType.getTag());
                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                // Union of unions, no need to handle already it becomes a single flattened union

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) memberType;
                    int dimention = Utils.getDimensions(arrayType);
                    fieldName = ARRAY_FIELD_NAME + SEPARATOR + dimention;
                    boolean isUnionMember = true;
                    boolean isRecordField = false;
                    generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldName, dimention, fieldNumber,
                            isUnionMember, isRecordField);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType recordType = (RecordType) memberType;
                    String nestedMessageName = recordType.getName();
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    fieldName = recordType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + memberType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }
    }

    private static void generateMessageDefinitionForArrayType(ProtobufMessageBuilder messageBuilder,
                                                              ArrayType arrayType, String fieldName, int fieldNumber,
                                                              int dimention, boolean isRecordField) {
        generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldName, dimention, fieldNumber, false,
                isRecordField);
    }

    private static void generateMessageDefinitionForArrayType(ProtobufMessageBuilder messageBuilder,
                                                              ArrayType arrayType, String fieldName, int fieldNumber) {
        generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldName, -1, fieldNumber, false, false);
    }

    private static void generateMessageDefinitionForArrayType(ProtobufMessageBuilder messageBuilder,
                                                              ArrayType arrayType, String fieldName, int dimension,
                                                              int fieldNumber, boolean isUnionMember,
                                                              boolean isRecordField) {
        Type elementType = arrayType.getElementType();

        switch (elementType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(elementType.getTag());
                String label = protoType.equals(BYTES) ? OPTIONAL_LABEL : REPEATED_LABEL;

                if (isUnionMember) {
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    fieldName = elementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageFieldBuilder messageField =
                        new ProtobufMessageFieldBuilder(label, protoType, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }


            case TypeTags.DECIMAL_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(elementType.getTag());

                if (isUnionMember) {
                    fieldName = elementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField =
                        new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.UNION_TAG: {
                String nestedMessageName = UNION_BUILDER_NAME;

                if (isUnionMember) {
                    String ballerinaType = Utils.getElementTypeOfBallerinaArray(arrayType);
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) elementType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField =
                        new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType nestedArrayType = (ArrayType) elementType;
                String nestedMessageName = ARRAY_BUILDER_NAME;

                if (isUnionMember) {
                    String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
                    nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                } else if (isRecordField) {
                    String ballerinaType = Utils.getElementTypeOfBallerinaArray(nestedArrayType);
                    nestedMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
                    nestedMessageName = ballerinaType + TYPE_SEPARATOR + nestedMessageName;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForArrayType(nestedMessageBuilder, nestedArrayType, ARRAY_FIELD_NAME, 1);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField =
                        new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) elementType;
                String nestedMessageName = recordType.getName();

                if (isUnionMember) {
                    String ballerinaType = recordType.getName();
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField =
                        new ProtobufMessageFieldBuilder(REPEATED_LABEL, nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + elementType.getName(), SERDES_ERROR);
        }
    }

    private static void generateMessageDefinitionForRecordType(ProtobufMessageBuilder messageBuilder,
                                                               RecordType recordType) {
        Map<String, Field> recordFields = recordType.getFields();
        int fieldNumber = 1;

        List<Field> fieldEntries =
                recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName)).collect(
                        Collectors.toList());

        for (Field fieldEntry : fieldEntries) {
            String fieldEntryName = fieldEntry.getFieldName();
            Type fieldEntryType = fieldEntry.getFieldType();

            switch (fieldEntryType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(fieldEntryType.getTag());
                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(fieldEntryType.getTag());
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    String nestedMessageName = fieldEntryName + TYPE_SEPARATOR + UNION_BUILDER_NAME;
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) fieldEntryType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName, fieldEntryName,
                                    fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) fieldEntryType;
                    int dimention = Utils.getDimensions(arrayType);
                    boolean isRecordField = true;
                    generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldEntryName, fieldNumber,
                            dimention, isRecordField);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType nestedRecordType = (RecordType) fieldEntryType;
                    String nestedMessageName = nestedRecordType.getName();
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, nestedRecordType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField =
                            new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, nestedMessageName, fieldEntryName,
                                    fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + fieldEntryType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }
    }
}
