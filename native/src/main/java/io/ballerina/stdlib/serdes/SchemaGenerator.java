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
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_MAP_AS_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_TABLE_AS_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.BYTES;
import static io.ballerina.stdlib.serdes.Constants.DECIMAL_VALUE;
import static io.ballerina.stdlib.serdes.Constants.FAILED_WRITE_FILE;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.MAP_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.PROTO3;
import static io.ballerina.stdlib.serdes.Constants.RECORD_FIELD_OF_MAP_ONLY_SUPPORTED_WITH_REFERENCE_TYPE;
import static io.ballerina.stdlib.serdes.Constants.RECORD_FIELD_OF_TABLE_ONLY_SUPPORTED_WITH_REFERENCE_TYPE;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_GENERATION_FAILURE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UINT32;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
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
                int fieldNumber = 1;
                messageName = Utils.createMessageName(referredType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveType(messageBuilder, referredType, ATOMIC_FIELD_NAME, fieldNumber);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                messageName = Utils.createMessageName(referredType.getName());
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForPrimitiveDecimal(messageBuilder);
                break;
            }

            case TypeTags.UNION_TAG: {
                messageName = UNION_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForUnionType(messageBuilder, (UnionType) referredType);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                int fieldNumber = 1;
                ArrayType arrayType = (ArrayType) referredType;
                messageName = ARRAY_BUILDER_NAME;
                messageBuilder = new ProtobufMessageBuilder(messageName);

                generateMessageDefinitionForArrayType(messageBuilder, arrayType, ARRAY_FIELD_NAME, fieldNumber);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) referredType;
                messageName = recordType.getName();
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForRecordType(messageBuilder, recordType);
                break;
            }

            case TypeTags.MAP_TAG: {
                MapType mapType = (MapType) referredType;
                messageName = MAP_BUILDER;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForMapType(messageBuilder, mapType);
                break;
            }

            case TypeTags.TABLE_TAG: {
                TableType tableType = (TableType) referredType;
                messageName = Constants.TABLE_BUILDER;
                messageBuilder = new ProtobufMessageBuilder(messageName);
                generateMessageDefinitionForTableType(messageBuilder, tableType);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }

        return messageBuilder;
    }

    // Generate schema for all ballerina primitive types except for decimal type
    private static void generateMessageDefinitionForPrimitiveType(ProtobufMessageBuilder messageBuilder,
                                                                  Type ballerinaType, String fieldName,
                                                                  int fieldNumber) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(ballerinaType.getTag());
        ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType, fieldName,
                fieldNumber);
        messageBuilder.addField(messageField);
    }

    // Generates schema for ballerina decimal type
    private static void generateMessageDefinitionForPrimitiveDecimal(ProtobufMessageBuilder messageBuilder) {
        int fieldNumber = 1;

        // Java BigDecimal representation used for serializing ballerina decimal value
        ProtobufMessageFieldBuilder scaleField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, SCALE,
                fieldNumber++);
        ProtobufMessageFieldBuilder precisionField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, UINT32, PRECISION,
                fieldNumber++);
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BYTES, VALUE,
                fieldNumber);

        messageBuilder.addField(scaleField);
        messageBuilder.addField(precisionField);
        messageBuilder.addField(valueField);
    }

    private static Map.Entry<String, Type> mapUnionMemberToMapEntry(Type type) {
        Type referredType = TypeUtils.getReferredType(type);
        String typeName = referredType.getName();

        if (referredType.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getDimensions((ArrayType) referredType);
            typeName = Utils.getElementTypeOfBallerinaArray((ArrayType) referredType);

            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention + TYPE_SEPARATOR
                    + UNION_FIELD_NAME;

            return Map.entry(key, type);
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)
                || referredType.getTag() == TypeTags.RECORD_TYPE_TAG) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, type);
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, type);
        }

        if (referredType.getTag() == TypeTags.MAP_TAG) {
            // TODO: support map member
            throw createSerdesError(MAP_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        if (referredType.getTag() == TypeTags.TABLE_TAG) {
            // TODO: support table member
            throw createSerdesError(Constants.TABLE_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
    }

    private static void generateMessageDefinitionForUnionType(ProtobufMessageBuilder messageBuilder,
                                                              UnionType unionType) {
        int fieldNumber = 1;

        List<Type> memberTypes = unionType.getMemberTypes().stream().map(SchemaGenerator::mapUnionMemberToMapEntry)
                .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());

        // Member field names are prefixed with ballerina type name to avoid name collision in proto message definition
        for (Type memberType : memberTypes) {
            Type referredMemberType = TypeUtils.getReferredType(memberType);
            String fieldName;

            switch (referredMemberType.getTag()) {
                case TypeTags.NULL_TAG: {
                    fieldName = NULL_FIELD_NAME;
                    ProtobufMessageFieldBuilder nilField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, BOOL,
                            fieldName, fieldNumber);
                    messageBuilder.addField(nilField);
                    break;
                }

                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    fieldName = referredMemberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    generateMessageDefinitionForPrimitiveType(messageBuilder, referredMemberType, fieldName,
                            fieldNumber);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(DECIMAL_VALUE);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    fieldName = referredMemberType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredMemberType.getTag());
                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            protoType, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                // Union of unions, no need to handle already it becomes a single flattened union

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) referredMemberType;
                    int dimention = Utils.getDimensions(arrayType);
                    fieldName = ARRAY_FIELD_NAME + SEPARATOR + dimention;
                    boolean isUnionMember = true;
                    boolean isRecordField = false;
                    generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldName, dimention, fieldNumber,
                            isUnionMember, isRecordField);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType recordType = (RecordType) referredMemberType;
                    String nestedMessageName = recordType.getName();

                    // Check for cyclic reference in ballerina record
                    boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
                    if (!hasMessageDefinition) {
                        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                                messageBuilder);
                        generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                        messageBuilder.addNestedMessage(nestedMessageBuilder);
                    }

                    fieldName = recordType.getName() + TYPE_SEPARATOR + UNION_FIELD_NAME;
                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            nestedMessageName, fieldName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                // TODO: support map member
                // TODO: support table member

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredMemberType.getName(), SERDES_ERROR);
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
        Type referredElementType = TypeUtils.getReferredType(elementType);

        switch (referredElementType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredElementType.getTag());
                String label = protoType.equals(BYTES) ? OPTIONAL_LABEL : REPEATED_LABEL;

                if (isUnionMember) {
                    // Field names and nested message names are prefixed with ballerina type to avoid name collision
                    fieldName = referredElementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR
                            + UNION_FIELD_NAME;
                }

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(label, protoType, fieldName,
                        fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }


            case TypeTags.DECIMAL_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredElementType.getTag());

                if (isUnionMember) {
                    fieldName = referredElementType.getName() + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR
                            + UNION_FIELD_NAME;
                }

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, protoType,
                        fieldName, fieldNumber);
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

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        messageBuilder);
                generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) referredElementType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType nestedArrayType = (ArrayType) referredElementType;
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

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        messageBuilder);
                generateMessageDefinitionForArrayType(nestedMessageBuilder, nestedArrayType, ARRAY_FIELD_NAME, 1);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType recordType = (RecordType) referredElementType;
                String nestedMessageName = recordType.getName();

                if (isUnionMember) {
                    String ballerinaType = recordType.getName();
                    fieldName = ballerinaType + TYPE_SEPARATOR + fieldName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                }

                // Check for cyclic reference in ballerina record
                boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);
                if (!hasMessageDefinition) {
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, recordType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);
                }

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.MAP_TAG: {
                MapType mapType = (MapType) referredElementType;

                if (isUnionMember) {
                    // TODO: support array of map union member
                    throw createSerdesError(ARRAY_OF_MAP_AS_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
                }

                String nestedMessageName = MAP_BUILDER;
                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForMapType(nestedMessageBuilder, mapType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            case TypeTags.TABLE_TAG: {
                TableType tableType = (TableType) referredElementType;

                if (isUnionMember) {
                    // TODO: support array of table union member
                    throw createSerdesError(ARRAY_OF_TABLE_AS_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
                }

                String nestedMessageName = TABLE_BUILDER;
                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName);
                generateMessageDefinitionForTableType(nestedMessageBuilder, tableType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, fieldName, fieldNumber);
                messageBuilder.addField(messageField);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredElementType.getName(), SERDES_ERROR);
        }
    }

    private static void generateMessageDefinitionForRecordType(ProtobufMessageBuilder messageBuilder,
                                                               RecordType recordType) {
        Map<String, Field> recordFields = recordType.getFields();
        int fieldNumber = 1;

        List<Field> fieldEntries = recordFields.values().stream().sorted(Comparator.comparing(Field::getFieldName))
                .collect(Collectors.toList());

        for (Field fieldEntry : fieldEntries) {
            String fieldEntryName = fieldEntry.getFieldName();
            Type fieldEntryType = fieldEntry.getFieldType();
            Type referredFieldEntryType = TypeUtils.getReferredType(fieldEntryType);

            switch (referredFieldEntryType.getTag()) {
                case TypeTags.INT_TAG:
                case TypeTags.BYTE_TAG:
                case TypeTags.FLOAT_TAG:
                case TypeTags.STRING_TAG:
                case TypeTags.BOOLEAN_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredFieldEntryType.getTag());
                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.DECIMAL_TAG: {
                    String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredFieldEntryType.getTag());
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                    generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            protoType, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.UNION_TAG: {
                    String nestedMessageName = fieldEntryName + TYPE_SEPARATOR + UNION_BUILDER_NAME;
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                            messageBuilder);
                    generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) referredFieldEntryType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            nestedMessageName, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.ARRAY_TAG: {
                    ArrayType arrayType = (ArrayType) referredFieldEntryType;
                    int dimention = Utils.getDimensions(arrayType);
                    boolean isRecordField = true;
                    generateMessageDefinitionForArrayType(messageBuilder, arrayType, fieldEntryName, fieldNumber,
                            dimention, isRecordField);
                    break;
                }

                case TypeTags.RECORD_TYPE_TAG: {
                    RecordType nestedRecordType = (RecordType) referredFieldEntryType;
                    String nestedMessageName = nestedRecordType.getName();
                    boolean hasMessageDefinition = messageBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);

                    // Check for cyclic reference in ballerina record
                    if (!hasMessageDefinition) {
                        ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                                messageBuilder);
                        generateMessageDefinitionForRecordType(nestedMessageBuilder, nestedRecordType);
                        messageBuilder.addNestedMessage(nestedMessageBuilder);
                    }

                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            nestedMessageName, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.MAP_TAG: {
                    if (fieldEntryType.getTag() != TypeTags.TYPE_REFERENCED_TYPE_TAG) {
                        throw createSerdesError(RECORD_FIELD_OF_MAP_ONLY_SUPPORTED_WITH_REFERENCE_TYPE, SERDES_ERROR);
                    }

                    String nestedMessageName = fieldEntryType.getName() + TYPE_SEPARATOR + MAP_BUILDER;
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                            messageBuilder);
                    generateMessageDefinitionForMapType(nestedMessageBuilder, (MapType) referredFieldEntryType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            nestedMessageName, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                case TypeTags.TABLE_TAG: {
                    if (fieldEntryType.getTag() != TypeTags.TYPE_REFERENCED_TYPE_TAG) {
                        throw createSerdesError(RECORD_FIELD_OF_TABLE_ONLY_SUPPORTED_WITH_REFERENCE_TYPE, SERDES_ERROR);
                    }

                    String nestedMessageName = fieldEntryType.getName() + TYPE_SEPARATOR + TABLE_BUILDER;
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                            messageBuilder);
                    generateMessageDefinitionForTableType(nestedMessageBuilder, (TableType) referredFieldEntryType);
                    messageBuilder.addNestedMessage(nestedMessageBuilder);

                    ProtobufMessageFieldBuilder messageField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                            nestedMessageName, fieldEntryName, fieldNumber);
                    messageBuilder.addField(messageField);
                    break;
                }

                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredFieldEntryType.getName(), SERDES_ERROR);
            }
            fieldNumber++;
        }
    }


    private static void generateMessageDefinitionForMapType(ProtobufMessageBuilder messageBuilder, MapType mapType) {
        ProtobufMessageBuilder mapEntryBuilder = new ProtobufMessageBuilder(MAP_FIELD_ENTRY);

        int keyFieldNumber = 1;
        int valueFieldNumber = 2;

        ProtobufMessageFieldBuilder keyField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, STRING, KEY_NAME,
                keyFieldNumber);
        mapEntryBuilder.addField(keyField);

        Type constrainedType = mapType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);

        switch (referredConstrainedType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.BYTE_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.STRING_TAG:
            case TypeTags.BOOLEAN_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredConstrainedType.getTag());
                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType,
                        VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            case TypeTags.DECIMAL_TAG: {
                String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(referredConstrainedType.getTag());
                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(protoType);
                generateMessageDefinitionForPrimitiveDecimal(nestedMessageBuilder);
                mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, protoType,
                        VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            case TypeTags.UNION_TAG: {
                String nestedMessageName = VALUE_NAME + TYPE_SEPARATOR + UNION_BUILDER_NAME;
                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        mapEntryBuilder);
                generateMessageDefinitionForUnionType(nestedMessageBuilder, (UnionType) referredConstrainedType);
                mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                        nestedMessageName, VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            case TypeTags.ARRAY_TAG: {
                ArrayType arrayType = (ArrayType) referredConstrainedType;
                int dimention = Utils.getDimensions(arrayType);
                boolean isRecordField = true;
                generateMessageDefinitionForArrayType(mapEntryBuilder, arrayType, VALUE_NAME, valueFieldNumber,
                        dimention, isRecordField);
                break;
            }

            case TypeTags.RECORD_TYPE_TAG: {
                RecordType nestedRecordType = (RecordType) referredConstrainedType;
                String nestedMessageName = nestedRecordType.getName();
                boolean hasMessageDefinition = mapEntryBuilder.hasMessageDefinitionInMessageTree(nestedMessageName);

                // Check for cyclic reference in ballerina record
                if (!hasMessageDefinition) {
                    ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                            mapEntryBuilder);
                    generateMessageDefinitionForRecordType(nestedMessageBuilder, nestedRecordType);
                    mapEntryBuilder.addNestedMessage(nestedMessageBuilder);
                }

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                        nestedMessageName, VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            case TypeTags.MAP_TAG: {
                MapType nestedMapType = (MapType) constrainedType;
                String nestedMessageName = MAP_BUILDER;

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        mapEntryBuilder);
                generateMessageDefinitionForMapType(nestedMessageBuilder, nestedMapType);
                mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                        nestedMessageName, VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            case TypeTags.TABLE_TAG: {
                TableType tableType = (TableType) constrainedType;
                String nestedMessageName = TABLE_BUILDER;

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        mapEntryBuilder);
                generateMessageDefinitionForTableType(nestedMessageBuilder, tableType);
                mapEntryBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL,
                        nestedMessageName, VALUE_NAME, valueFieldNumber);
                mapEntryBuilder.addField(valueField);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredConstrainedType.getName(), SERDES_ERROR);
        }

        messageBuilder.addNestedMessage(mapEntryBuilder);
        ProtobufMessageFieldBuilder mapEntryField = new ProtobufMessageFieldBuilder(REPEATED_LABEL, MAP_FIELD_ENTRY,
                MAP_FIELD, 1);
        messageBuilder.addField(mapEntryField);
    }

    private static void generateMessageDefinitionForTableType(ProtobufMessageBuilder messageBuilder,
                                                              TableType tableType) {
        int entryFieldNumber = 1;
        Type constrainedType = tableType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);

        switch (referredConstrainedType.getTag()) {
            case TypeTags.RECORD_TYPE_TAG: {
                RecordType nestedRecordType = (RecordType) referredConstrainedType;
                String nestedMessageName = nestedRecordType.getName();

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        messageBuilder);
                generateMessageDefinitionForRecordType(nestedMessageBuilder, nestedRecordType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, TABLE_ENTRY, entryFieldNumber);
                messageBuilder.addField(valueField);
                break;
            }

            case TypeTags.MAP_TAG: {
                MapType nestedMapType = (MapType) constrainedType;
                String nestedMessageName = MAP_BUILDER;

                ProtobufMessageBuilder nestedMessageBuilder = new ProtobufMessageBuilder(nestedMessageName,
                        messageBuilder);
                generateMessageDefinitionForMapType(nestedMessageBuilder, nestedMapType);
                messageBuilder.addNestedMessage(nestedMessageBuilder);

                ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                        nestedMessageName, TABLE_ENTRY, entryFieldNumber);
                messageBuilder.addField(valueField);
                break;
            }

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredConstrainedType.getName(), SERDES_ERROR);
        }

    }
}
