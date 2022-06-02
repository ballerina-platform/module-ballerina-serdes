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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Deserializer class to generate Ballerina value from byte array.
 */
public class Deserializer {

    static final String ATOMIC_FIELD_NAME = "atomicField";
    static final String ARRAY_FIELD_NAME = "arrayfield";
    static final String NULL_FIELD_NAME = "nullField";

    static final String SCHEMA_NAME = "schema";

    static final String UNION_FIELD_NAME = "unionelement";
    static final String UNION_TYPE_IDENTIFIER = "ballerinauniontype";
    static final String UNION_FIELD_SEPARATOR = "__";

    static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
    static final String DESERIALIZATION_ERROR_MESSAGE = "Failed to Deserialize data: ";
    static final String MISSING_ENTRY_IN_DATATYPE = "Missing entry in datatype for ";
    static final String UNSUPPORTED_UNION_TYPE = "Unsupported union type";

    /**
     * Creates an anydata object from a byte array after deserializing.
     *
     * @param des  Deserializer object.
     * @param encodedMessage Byte array corresponding to encoded data.
     * @param dataType Data type of the encoded value.
     * @return anydata object.
     */
    public static Object deserialize(BObject des, BArray encodedMessage, BTypedesc dataType) {
        Descriptor schema = (Descriptor) des.getNativeData(SCHEMA_NAME);
        Object object = null;
        
        try {
            DynamicMessage dynamicMessage = generateDynamicMessageFromBytes(schema, encodedMessage);
            object = dynamicMessageToBallerinaType(dynamicMessage, dataType, schema);
        } catch (BError e) {
            return e;
        } catch (InvalidProtocolBufferException e) {
            return createSerdesError(DESERIALIZATION_ERROR_MESSAGE + e.getMessage(), SERDES_ERROR);
        }
        return object;
    }

    private static DynamicMessage generateDynamicMessageFromBytes(Descriptor schema, BArray encodedMessage)
                                                                                throws InvalidProtocolBufferException {
        return DynamicMessage.parseFrom(schema, encodedMessage.getBytes());
    }

    private static Object dynamicMessageToBallerinaType(DynamicMessage dynamicMessage, BTypedesc typedesc,
                                                        Descriptor schema) {
        Type type = typedesc.getDescribingType();

        if (type.getTag() <= TypeTags.BOOLEAN_TAG) {
            FieldDescriptor fieldDescriptor = schema.findFieldByName(ATOMIC_FIELD_NAME);
            return getBallerinaPrimitiveValueFromMessage(dynamicMessage.getField(fieldDescriptor), type.getTag());
        } else if (type.getTag() == TypeTags.UNION_TAG) {
            FieldDescriptor fieldDescriptor = schema.findFieldByName(ATOMIC_FIELD_NAME);
            DynamicMessage dynamicMessageForUnion = (DynamicMessage) dynamicMessage.getField(fieldDescriptor);
            return getBallerinaUnionTypeValueFromMessage(dynamicMessageForUnion, type, schema);
        } else if (type.getTag() == TypeTags.ARRAY_TAG) {
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getElementType();
            FieldDescriptor fieldDescriptor = schema.findFieldByName(ARRAY_FIELD_NAME);
            schema = fieldDescriptor.getContainingType();
            return getBallerinaArrayValueFromMessage(dynamicMessage.getField(fieldDescriptor), elementType, schema, 1);
        } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            Map<String, Object> mapObject = getBallerinaRecordValueFromMessage(dynamicMessage, type, schema);
            return ValueCreator.createRecordValue(type.getPackage(), type.getName(), mapObject);
        } else {
            throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    private static Object getBallerinaPrimitiveValueFromMessage(Object value) {
        return getBallerinaPrimitiveValueFromMessage(value, 0);
    }

    private static Object getBallerinaPrimitiveValueFromMessage(Object value, int typeTag) {
        if (typeTag == TypeTags.DECIMAL_TAG) {
            return ValueCreator.createDecimalValue(value.toString());
        }

        if (value instanceof String) {
            return StringUtils.fromString((String) value);
        }
        return value;
    }

    private static Object getBallerinaArrayValueFromMessage(Object value, Type type, Descriptor schema,
                                                            int unionFieldIdentifier) {
        if (value instanceof ByteString) {
            ByteString byteString = (ByteString) value;
            return ValueCreator.createArrayValue(byteString.toByteArray());
        } else {
            Collection collection = (Collection) value;
            BArray bArray = ValueCreator.createArrayValue(TypeCreator.createArrayType(type));
            for (Object element : collection) {
                if (type.getTag() == TypeTags.DECIMAL_TAG) {
                    bArray.append(ValueCreator.createDecimalValue(element.toString()));
                } else if (type.getTag() == TypeTags.STRING_TAG) {
                    bArray.append(StringUtils.fromString((String) element));
                } else if (type.getTag() == TypeTags.ARRAY_TAG) {
                    ArrayType arrayType = (ArrayType) type;
                    Type elementType = arrayType.getElementType();
                    String fieldName;
                    if (elementType.getTag() == TypeTags.UNION_TAG) {
                        fieldName = UNION_FIELD_NAME + unionFieldIdentifier;
                    } else if (elementType.getTag() == TypeTags.ARRAY_TAG) {
                        fieldName = UNION_FIELD_NAME + unionFieldIdentifier;
                        unionFieldIdentifier++;
                    } else {
                        fieldName = elementType.getName();
                    }
                    Descriptor nestedSchema = schema.findNestedTypeByName(fieldName);
                    DynamicMessage nestedDynamicMessage = (DynamicMessage) element;
                    FieldDescriptor fieldDescriptor = nestedSchema.findFieldByName(fieldName);
                    Object nestedArrayContent = nestedDynamicMessage.getField(fieldDescriptor);
                    BArray nestedArray = (BArray) getBallerinaArrayValueFromMessage(nestedArrayContent, elementType,
                                                                                    nestedSchema, unionFieldIdentifier);
                    bArray.append(nestedArray);
                } else if (type.getTag() == TypeTags.UNION_TAG) {
                    DynamicMessage dynamicMessageForUnion = (DynamicMessage) element;
                    bArray.append(getBallerinaUnionTypeValueFromMessage(dynamicMessageForUnion, type, schema));
                } else if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
                    Map<String, Object> mapObject = getBallerinaRecordValueFromMessage((DynamicMessage) element, type,
                                                                                       schema);
                    bArray.append(ValueCreator.createRecordValue(type.getPackage(), type.getName(), mapObject));
                } else {
                    bArray.append(element);
                }
            }
            return bArray;
        }
    }

    private static Map<String, Object> getBallerinaRecordValueFromMessage(DynamicMessage dynamicMessage, Type type,
                                                                          Descriptor schema) {
        Map<String, Object> map = new HashMap();
        BMap<BString, Object> typedescs = type.getEmptyValue();

        for (Map.Entry<FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            String fieldName = entry.getKey().getName();
            Object value = entry.getValue();
            if (value instanceof DynamicMessage) {
                DynamicMessage nestedDynamicMessage = (DynamicMessage) value;
                String fieldType = nestedDynamicMessage.getDescriptorForType().getName();
                String[] processFieldName = fieldType.split("_");
                String unionCheck = processFieldName[processFieldName.length - 1];
                if (unionCheck.contains(UNION_TYPE_IDENTIFIER)) {
                    Descriptor unionSchema = schema.findNestedTypeByName(fieldType);
                    Type unionType = null;
                    RecordType recordType = (RecordType) type;
                    for (Map.Entry<String, Field> member: recordType.getFields().entrySet()) {
                        if (member.getKey().equals(fieldName)) {
                            unionType = member.getValue().getFieldType();
                            break;
                        }
                    }
                    map.put(fieldName, getBallerinaUnionTypeValueFromMessage(nestedDynamicMessage, unionType,
                                                                             unionSchema));
                } else {
                    Map<String, Object> nestedMap = getBallerinaRecordValueFromMessage(nestedDynamicMessage, type,
                                                                                       schema);
                    String recordTypeName = getRecordTypeName(type, fieldName);
                    BMap<BString, Object> nestedRecord = ValueCreator.createRecordValue(type.getPackage(),
                                                                                        recordTypeName, nestedMap);
                    map.put(fieldName, nestedRecord);
                }
            } else if (value instanceof ByteString || entry.getKey().isRepeated()) {
                if (!(value instanceof ByteString)) {
                    Type elementType = getArrayElementType(type, fieldName);
                    Object handleArray = getBallerinaArrayValueFromMessage(value, elementType, schema, 1);
                    map.put(fieldName, handleArray);
                } else {
                    Object handleArray = getBallerinaArrayValueFromMessage(value, type, schema, 1);
                    map.put(fieldName, handleArray);
                }
            } else if (DataTypeMapper.getProtoTypeFromJavaType(value.getClass().getSimpleName()) != null) {
                Object key = StringUtils.fromString(fieldName);
                Object handlePrimitive;
                try {
                    int typeTag = ((BValue) typedescs.get(key)).getType().getTag();
                    handlePrimitive = getBallerinaPrimitiveValueFromMessage(value, typeTag);
                } catch (ClassCastException | NullPointerException e) {
                    handlePrimitive = getBallerinaPrimitiveValueFromMessage(value);
                }
                map.put(fieldName, handlePrimitive);
            } else {
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + value.getClass().getSimpleName(), SERDES_ERROR);
            }
        }
        return map;
    }

    private static String getRecordTypeName(Type type, String fieldName) {
        RecordType recordType = (RecordType) type;

        for (Map.Entry<String, Field> entry: recordType.getFields().entrySet()) {
            Type fieldType = entry.getValue().getFieldType();
            if (fieldType.getTag() == TypeTags.RECORD_TYPE_TAG && entry.getKey().equals(fieldName)) {
                return fieldType.getName();
            } else if (fieldType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                return getRecordTypeName(fieldType, fieldName);
            }
        }
        throw createSerdesError(DESERIALIZATION_ERROR_MESSAGE + MISSING_ENTRY_IN_DATATYPE + fieldName, SERDES_ERROR);
    }

    private static Type getArrayElementType(Type type, String fieldName) {
        RecordType recordType = (RecordType) type;

        for (Map.Entry<String, Field> entry: recordType.getFields().entrySet()) {
            if (entry.getValue().getFieldType().getTag() == TypeTags.ARRAY_TAG && entry.getKey().equals(fieldName)) {
                ArrayType arrayType = (ArrayType) entry.getValue().getFieldType();
                return arrayType.getElementType();
            } else if (entry.getValue().getFieldType().getTag() == TypeTags.RECORD_TYPE_TAG) {
                return getArrayElementType(entry.getValue().getFieldType(), fieldName);
            }
        }
        throw createSerdesError(DESERIALIZATION_ERROR_MESSAGE + MISSING_ENTRY_IN_DATATYPE + fieldName, SERDES_ERROR);
    }

    private static Object getBallerinaUnionTypeValueFromMessage(DynamicMessage dynamicMessage, Type type,
                                                                Descriptor schema) {
        for (Map.Entry<FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            Object value = entry.getValue();
            if (entry.getKey().getName().equals(NULL_FIELD_NAME) && (Boolean) value) {
                return null;
            }
            if (value instanceof DynamicMessage) {
                DynamicMessage dynamicMessageForUnion = (DynamicMessage) entry.getValue();
                Type recordType = getCorrespondingElementTypeFromUnion(type, entry.getKey().getName());
                Map<String, Object> mapObject = getBallerinaRecordValueFromMessage(dynamicMessageForUnion, recordType,
                                                                                   schema);
                return ValueCreator.createRecordValue(recordType.getPackage(), recordType.getName(), mapObject);
            } else if (value instanceof ByteString || entry.getKey().isRepeated()) {
                Type elementType = getCorrespondingElementTypeFromUnion(type, entry.getKey().getName());
                return getBallerinaArrayValueFromMessage(value, elementType, schema, 1);
            } else {
                return getBallerinaPrimitiveValueFromMessage(entry.getValue());
            }
        }
        throw createSerdesError(DESERIALIZATION_ERROR_MESSAGE + UNSUPPORTED_UNION_TYPE, SERDES_ERROR);
    }

    private static Type getCorrespondingElementTypeFromUnion(Type type, String fieldName) {
        UnionType unionType = (UnionType) type;
        String typeFromFieldName = fieldName.split(UNION_FIELD_SEPARATOR)[0];

        for (Type memberType : unionType.getMemberTypes()) {
            if (memberType.getTag() == TypeTags.ARRAY_TAG) {
                ArrayType arrayType = (ArrayType) memberType;
                String elementType;
                if (DataTypeMapper.getProtoTypeFromTag(arrayType.getElementType().getTag()) != null) {
                    elementType = DataTypeMapper.getProtoTypeFromTag(arrayType.getElementType().getTag());
                } else {
                    elementType = arrayType.getElementType().getName();
                }
                if (typeFromFieldName.equals(elementType)) {
                    return arrayType.getElementType();
                }
            } else if (memberType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                RecordType recordType = (RecordType) memberType;
                if (recordType.getName().equals(typeFromFieldName)) {
                    return recordType;
                }
            }
        }
        throw createSerdesError(DESERIALIZATION_ERROR_MESSAGE + UNSUPPORTED_UNION_TYPE, SERDES_ERROR);
    }
}
