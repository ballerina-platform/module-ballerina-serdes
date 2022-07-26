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
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;

import static io.ballerina.stdlib.serdes.Constants.ATOMIC_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BALLERINA_TYPEDESC_ATTRIBUTE_NAME;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.SCHEMA_NAME;
import static io.ballerina.stdlib.serdes.Constants.SERIALIZATION_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.TYPE_MISMATCH_ERROR_MESSAGE;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * Serializer class to create a byte array for a value.
 */
public class Serializer {

    /**
     * Creates a BArray for given data after serializing.
     *
     * @param ser     Serializer object.
     * @param anydata Data that is being serialized.
     * @return Byte array of the serialized value.
     */
    @SuppressWarnings("unused")
    public static Object serialize(BObject ser, Object anydata) {
        BTypedesc bTypedesc = (BTypedesc) ser.get(BALLERINA_TYPEDESC_ATTRIBUTE_NAME);
        Descriptor messageDescriptor = (Descriptor) ser.getNativeData(SCHEMA_NAME);
        DynamicMessage dynamicMessage;
        try {
            dynamicMessage = buildDynamicMessageFromType(anydata, messageDescriptor,
                    bTypedesc.getDescribingType()).build();
        } catch (BError ballerinaError) {
            return ballerinaError;
        } catch (IllegalArgumentException e) {
            String errorMessage = SERIALIZATION_ERROR_MESSAGE + TYPE_MISMATCH_ERROR_MESSAGE;
            return createSerdesError(errorMessage, SERDES_ERROR);
        }
        return ValueCreator.createArrayValue(dynamicMessage.toByteArray());
    }

    private static Builder buildDynamicMessageFromType(Object anydata, Descriptor messageDescriptor,
                                                       Type ballerinaType) {
        Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor);
        Type referredType = TypeUtils.getReferredType(ballerinaType);

        switch (referredType.getTag()) {
            case TypeTags.INT_TAG:
            case TypeTags.FLOAT_TAG:
            case TypeTags.BOOLEAN_TAG:
                return generateMessageForPrimitiveType(messageBuilder, anydata);

            case TypeTags.BYTE_TAG:
                byte[] bytes = new byte[]{((Integer) anydata).byteValue()};
                return generateMessageForPrimitiveType(messageBuilder, bytes);

            case TypeTags.STRING_TAG:
                String string = ((BString) anydata).getValue();
                return generateMessageForPrimitiveType(messageBuilder, string);

            case TypeTags.DECIMAL_TAG:
                return generateMessageForPrimitiveDecimalType(messageBuilder, anydata);

            case TypeTags.ARRAY_TAG:
            case TypeTags.UNION_TAG:
            case TypeTags.RECORD_TYPE_TAG:
            case TypeTags.MAP_TAG:
            case TypeTags.TABLE_TAG:
            case TypeTags.TUPLE_TAG:
                BallerinaStructuredTypeMessageSerializer structuredTypeMessageSerializer
                        = new BallerinaStructuredTypeMessageSerializer(referredType, anydata, messageBuilder);
                return structuredTypeMessageSerializer.generateMessage();

            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
        }
    }

    private static Builder generateMessageForPrimitiveType(Builder messageBuilder, Object fieldValue) {
        Descriptor messageDescriptor = messageBuilder.getDescriptorForType();
        FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(ATOMIC_FIELD_NAME);
        messageBuilder.setField(fieldDescriptor, fieldValue);
        return messageBuilder;
    }

    private static Builder generateMessageForPrimitiveDecimalType(Builder messageBuilder, Object anydata) {
        BigDecimal bigDecimal = ((BDecimal) anydata).decimalValue();

        Descriptor decimalSchema = messageBuilder.getDescriptorForType();
        FieldDescriptor scale = decimalSchema.findFieldByName(SCALE);
        FieldDescriptor precision = decimalSchema.findFieldByName(PRECISION);
        FieldDescriptor value = decimalSchema.findFieldByName(VALUE);

        messageBuilder.setField(scale, bigDecimal.scale());
        messageBuilder.setField(precision, bigDecimal.precision());
        messageBuilder.setField(value, bigDecimal.unscaledValue().toByteArray());
        return messageBuilder;
    }
}
