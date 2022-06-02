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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;

import java.util.Locale;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Serializer class to create a byte array for a value.
 */
public class Serializer {

    static final String SCHEMA_NAME = "schema";

    static final String ATOMIC_FIELD_NAME = "atomicField";
    static final String ARRAY_FIELD_NAME = "arrayfield";
    static final String UNION_FIELD_NAME = "UnionField";
    static final String NULL_FIELD_NAME = "nullField";

    static final String NESTED_UNION_FIELD_NAME = "unionelement";
    static final String UNION_TYPE_IDENTIFIER = "ballerinauniontype";
    static final String UNION_FIELD_SEPARATOR = "__";

    static final String STRING = "string";
    static final String RECORD = "_record";
    static final String MESSAGE = "message";

    static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
    static final String SERIALIZATION_ERROR_MESSAGE = "Failed to Serialize data: ";
    static final String TYPE_MISMATCH_ERROR_MESSAGE = "Type mismatch";

    static final BString BALLERINA_TYPEDESC_ATTRIBUTE_NAME = StringUtils.fromString("dataType");

    /**
     * Creates a BArray for given data after serializing.
     *
     * @param serializer  Serializer object.
     * @param message Data that is being serialized.
     * @return Byte array of the serialized value.
     */
    public static Object serialize(BObject serializer, Object message) {
        BTypedesc dataType = (BTypedesc) serializer.get(BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
        Descriptor schema = (Descriptor) serializer.getNativeData(SCHEMA_NAME);
        DynamicMessage dynamicMessage;
        try {
            dynamicMessage = generateDynamicMessageForBallerinaValue(message, schema, dataType);
        } catch (BError e) {
            return e;
        } catch (IllegalArgumentException e) {
            return createSerdesError(SERIALIZATION_ERROR_MESSAGE + TYPE_MISMATCH_ERROR_MESSAGE, SERDES_ERROR);
        }
        return ValueCreator.createArrayValue(dynamicMessage.toByteArray());
    }

    private static DynamicMessage generateDynamicMessageForBallerinaValue(Object dataObject, Descriptor schema,
                                                                          BTypedesc bTypedesc) {
        Type type = bTypedesc.getDescribingType();

        if (type.getTag() <= TypeTags.BOOLEAN_TAG) {
            DynamicMessage.Builder newMessageFromSchema = DynamicMessage.newBuilder(schema);
            Descriptor messageDescriptor = newMessageFromSchema.getDescriptorForType();
            FieldDescriptor field = messageDescriptor.findFieldByName(ATOMIC_FIELD_NAME);
            generateDynamicMessageForBallerinaPrimitiveValue(newMessageFromSchema, field, dataObject, type.getTag());
            return  newMessageFromSchema.build();
        } else if (type.getTag() == TypeTags.UNION_TAG) {
            DynamicMessage.Builder newMessageFromSchema = DynamicMessage.newBuilder(schema);
            Descriptor messageDescriptor = newMessageFromSchema.getDescriptorForType();
            Descriptor unionSchema = schema.findNestedTypeByName(UNION_FIELD_NAME);
            DynamicMessage nestedMessage = generateDynamicMessageForBallerinaUnion(dataObject, unionSchema,
                                                                                   UNION_FIELD_NAME);
            FieldDescriptor field = messageDescriptor.findFieldByName(ATOMIC_FIELD_NAME);
            newMessageFromSchema.setField(field, nestedMessage);
            return newMessageFromSchema.build();
        } else if (type.getTag() == TypeTags.ARRAY_TAG) {
            DynamicMessage.Builder newMessageFromSchema = DynamicMessage.newBuilder(schema);
            Descriptor messageDescriptor = newMessageFromSchema.getDescriptorForType();
            FieldDescriptor field = messageDescriptor.findFieldByName(ARRAY_FIELD_NAME);
            generateDynamicMessageForBallerinaArray(newMessageFromSchema, schema, field, dataObject, 1);
            return  newMessageFromSchema.build();
        } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            return generateDynamicMessageForBallerinaRecord((BMap<BString, Object>) dataObject, schema);
        } else {
            throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    private static void generateDynamicMessageForBallerinaPrimitiveValue(DynamicMessage.Builder messageBuilder,
                                                                         FieldDescriptor field, Object value) {
        generateDynamicMessageForBallerinaPrimitiveValue(messageBuilder, field, value, 0);
    }

    private static void generateDynamicMessageForBallerinaPrimitiveValue(DynamicMessage.Builder messageBuilder,
                                                                         FieldDescriptor field, Object value,
                                                                         int typeTag) {
        String fieldType = DataTypeMapper.getProtoTypeFromJavaType(value.getClass().getSimpleName());

        if (typeTag == TypeTags.DECIMAL_TAG) {
            messageBuilder.setField(field,  value.toString());
        } else if (STRING.equals(fieldType)) {
            BString bString = (BString) value;
            messageBuilder.setField(field, bString.getValue());
        } else {
            messageBuilder.setField(field, value);
        }
    }

    private static void generateDynamicMessageForBallerinaArray(DynamicMessage.Builder messageBuilder,
                                                                Descriptor schema, FieldDescriptor field, Object value,
                                                                int unionFieldIdentifier) {
        BArray bArray = (BArray) value;
        long len = bArray.size();
        Type type = bArray.getElementType();
        String fieldType = field.getType().name().toLowerCase(Locale.ROOT);

        if (type.getTag() == TypeTags.BYTE_TAG) {
            messageBuilder.setField(field, bArray.getBytes());
            return;
        }

        for (long i = 0; i < len; i++) {
            Object element = bArray.get(i);
            boolean isUnion;
            try {
                isUnion = field.getMessageType().getName().toLowerCase(Locale.ROOT).contains(UNION_TYPE_IDENTIFIER);
            } catch (Exception e) {
                isUnion = false;
            }
            if (fieldType.equals(MESSAGE) && isUnion) {
                String nestedTypeName = field.getMessageType().getName();
                Descriptor elementSchema = field.getContainingType().findNestedTypeByName(nestedTypeName);
                String fieldName = nestedTypeName.toLowerCase(Locale.ROOT);
                DynamicMessage elementDynamicMessage = generateDynamicMessageForBallerinaUnion(element, elementSchema
                        , fieldName);
                messageBuilder.addRepeatedField(field, elementDynamicMessage);
            } else {
                if (type.getTag() == TypeTags.DECIMAL_TAG && fieldType.equals(STRING)) {
                    messageBuilder.addRepeatedField(field, element.toString());
                } else if (fieldType.equals(STRING)) {
                    BString bString = (BString) element;
                    messageBuilder.addRepeatedField(field, bString.getValue());
                } else if (type.getTag() == TypeTags.ARRAY_TAG) {
                    BArray nestedArray = (BArray) element;
                    String nestedTypeName;
                    if (nestedArray.getElementType().getTag() == TypeTags.UNION_TAG) {
                        nestedTypeName = NESTED_UNION_FIELD_NAME + unionFieldIdentifier;
                    } else if (nestedArray.getElementType().getTag() == TypeTags.ARRAY_TAG) {
                        nestedTypeName = NESTED_UNION_FIELD_NAME + unionFieldIdentifier;
                        unionFieldIdentifier++;
                    } else {
                        nestedTypeName = nestedArray.getElementType().getName();
                    }
                    Descriptor nestedSchema = schema.findNestedTypeByName(nestedTypeName);
                    DynamicMessage.Builder nestedMessage = DynamicMessage.newBuilder(nestedSchema);
                    Descriptor messageDescriptor = nestedMessage.getDescriptorForType();
                    FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(nestedTypeName);
                    generateDynamicMessageForBallerinaArray(nestedMessage, nestedSchema, fieldDescriptor, element,
                                                            unionFieldIdentifier);
                    messageBuilder.addRepeatedField(field, nestedMessage.build());
                } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
                    String nestedTypeName = bArray.getElementType().getName();
                    Descriptor elementSchema = field.getContainingType().findNestedTypeByName(nestedTypeName);
                    DynamicMessage elementDynamicMessage = generateDynamicMessageForBallerinaRecord(
                            (BMap<BString, Object>) element, elementSchema);
                    messageBuilder.addRepeatedField(field, elementDynamicMessage);
                } else {
                    messageBuilder.addRepeatedField(field, element);
                }
            }
        }
    }

    private static DynamicMessage generateDynamicMessageForBallerinaRecord(BMap<BString, Object> bMap,
                                                                           Descriptor schema) {
        DynamicMessage.Builder newMessageFromSchema = DynamicMessage.newBuilder(schema);
        Descriptor messageDescriptor = newMessageFromSchema.getDescriptorForType();

        for (Map.Entry<BString, Object> entry : bMap.entrySet()) {
            String fieldName = entry.getKey().getValue();
            Object value = entry.getValue();
            String unionType = fieldName + "_" + UNION_TYPE_IDENTIFIER;
            Descriptor unionDescriptor = messageDescriptor.findNestedTypeByName(unionType);
            if (unionDescriptor != null) {
                DynamicMessage nestedMessage = generateDynamicMessageForBallerinaUnion(value, unionDescriptor,
                                                                                       unionType);
                newMessageFromSchema.setField(messageDescriptor.findFieldByName(fieldName), nestedMessage);
            } else {
                if (value == null) {
                    continue;
                }
                if (value instanceof BMap) {
                    BMap<BString, Object> objToBMap = (BMap<BString, Object>) value;
                    String nestedTypeName = objToBMap.getType().getName();
                    Descriptor subMessageDescriptor = schema.findNestedTypeByName(nestedTypeName);
                    nestedTypeName = entry.getKey().getValue();
                    DynamicMessage nestedMessage = generateDynamicMessageForBallerinaRecord(objToBMap,
                                                                                            subMessageDescriptor);
                    newMessageFromSchema.setField(messageDescriptor.findFieldByName(nestedTypeName), nestedMessage);
                } else if (value instanceof BArray) {
                    FieldDescriptor field = messageDescriptor.findFieldByName(fieldName);
                    generateDynamicMessageForBallerinaArray(newMessageFromSchema, schema, field, value, 1);
                } else if (DataTypeMapper.getProtoTypeFromJavaType(value.getClass().getSimpleName()) != null) {
                    FieldDescriptor field = messageDescriptor.findFieldByName(fieldName);
                    try {
                        int typeTag = ((BValue) value).getType().getTag();
                        generateDynamicMessageForBallerinaPrimitiveValue(newMessageFromSchema, field, value, typeTag);
                    } catch (ClassCastException e) {
                        generateDynamicMessageForBallerinaPrimitiveValue(newMessageFromSchema, field, value);
                    }
                } else {
                    String dataType = value.getClass().getSimpleName();
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + dataType, SERDES_ERROR);
                }
            }
        }
        return newMessageFromSchema.build();
    }

    private static DynamicMessage generateDynamicMessageForBallerinaUnion(Object value, Descriptor schema,
                                                                          String name) {
        DynamicMessage.Builder newMessageFromSchema = DynamicMessage.newBuilder(schema);
        Descriptor messageDescriptor = newMessageFromSchema.getDescriptorForType();

        if (value == null) {
            FieldDescriptor field = messageDescriptor.findFieldByName(NULL_FIELD_NAME);
            newMessageFromSchema.setField(field, true);
            return newMessageFromSchema.build();
        }
        // TODO: Find a better approach instead of using the class name
        String dataType = value.getClass().getSimpleName();
        String ballerinaToProtoMap = DataTypeMapper.getProtoTypeFromJavaType(dataType);
        if (ballerinaToProtoMap != null) {
            String fieldName = ballerinaToProtoMap + UNION_FIELD_SEPARATOR + name;
            FieldDescriptor field = messageDescriptor.findFieldByName(fieldName);
            generateDynamicMessageForBallerinaPrimitiveValue(newMessageFromSchema, field, value);
            return newMessageFromSchema.build();
        }
        if (value instanceof BArray) {
            BArray bArray = (BArray) value;
            String elementType = DataTypeMapper.getProtoTypeFromTag(bArray.getElementType().getTag());
            if (elementType == null) {
                elementType = bArray.getElementType().getName();
            }
            String fieldName = elementType + UNION_FIELD_SEPARATOR + "array_" + name;
            FieldDescriptor field = messageDescriptor.findFieldByName(fieldName);
            generateDynamicMessageForBallerinaArray(newMessageFromSchema, schema, field, value, 1);
        } else {
            BMap<BString, Object> bMap = (BMap<BString, Object>) value;
            String elementType = bMap.getTypedesc().getDescribingType().getName();
            String fieldName = elementType + UNION_FIELD_SEPARATOR + name;
            Descriptor nestedSchema = schema.findNestedTypeByName(elementType);
            if (nestedSchema == null) {
                nestedSchema = schema.findNestedTypeByName(elementType + RECORD);
            }
            DynamicMessage dynamicMessage = generateDynamicMessageForBallerinaRecord(bMap, nestedSchema);
            FieldDescriptor field = messageDescriptor.findFieldByName(fieldName);
            newMessageFromSchema.setField(field, dynamicMessage);
        }
        return  newMessageFromSchema.build();
    }
}
