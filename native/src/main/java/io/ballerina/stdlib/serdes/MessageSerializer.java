/*
 * Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.math.BigDecimal;
import java.util.List;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.PRECISION;
import static io.ballerina.stdlib.serdes.Constants.SCALE;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Constants.VALUE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * {@link MessageSerializer} provides generic functions for concrete message serializers.
 */
public abstract class MessageSerializer {
    private final Builder dynamicMessageBuilder;
    private final Object anydata;
    private final BallerinaStructuredTypeMessageSerializer ballerinaStructuredTypeMessageSerializer;
    private String currentFieldName;

    public MessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                             BallerinaStructuredTypeMessageSerializer ballerinaStructuredTypeMessageSerializer) {
        this.dynamicMessageBuilder = dynamicMessageBuilder;
        this.anydata = anydata;
        this.ballerinaStructuredTypeMessageSerializer = ballerinaStructuredTypeMessageSerializer;
    }

    public Builder getDynamicMessageBuilder() {
        return dynamicMessageBuilder;
    }

    public BallerinaStructuredTypeMessageSerializer getBallerinaStructuredTypeMessageSerializer() {
        return ballerinaStructuredTypeMessageSerializer;
    }

    public Object getAnydata() {
        return anydata;
    }

    public String getCurrentFieldName() {
        return currentFieldName;
    }

    public void setCurrentFieldName(String currentFieldName) {
        this.currentFieldName = currentFieldName;
    }

    public FieldDescriptor getCurrentFieldDescriptor() {
        return dynamicMessageBuilder.getDescriptorForType().findFieldByName(currentFieldName);
    }

    public void setIntFieldValue(Object ballerinaInt) {
        setCurrentFieldValueInDynamicMessageBuilder(ballerinaInt);
    }

    public void setByteFieldValue(Integer ballerinaByte) {
        byte[] byteValue = new byte[]{ballerinaByte.byteValue()};
        setCurrentFieldValueInDynamicMessageBuilder(byteValue);
    }

    public void setFloatFieldValue(Object ballerinaFloat) {
        setCurrentFieldValueInDynamicMessageBuilder(ballerinaFloat);
    }

    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        Builder decimalMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        DynamicMessage decimalMessage = generateDecimalValueMessage(decimalMessageBuilder, ballerinaDecimal);
        setCurrentFieldValueInDynamicMessageBuilder(decimalMessage);
    }

    public DynamicMessage generateDecimalValueMessage(Builder decimalMessageBuilder, Object decimal) {
        BigDecimal bigDecimal = ((BDecimal) decimal).decimalValue();
        Descriptor decimalSchema = decimalMessageBuilder.getDescriptorForType();

        FieldDescriptor scale = decimalSchema.findFieldByName(SCALE);
        FieldDescriptor precision = decimalSchema.findFieldByName(PRECISION);
        FieldDescriptor value = decimalSchema.findFieldByName(VALUE);

        decimalMessageBuilder.setField(scale, bigDecimal.scale());
        decimalMessageBuilder.setField(precision, bigDecimal.precision());
        decimalMessageBuilder.setField(value, bigDecimal.unscaledValue().toByteArray());
        return decimalMessageBuilder.build();
    }

    public void setStringFieldValue(BString ballerinaString) {
        setCurrentFieldValueInDynamicMessageBuilder(ballerinaString.getValue());
    }

    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        setCurrentFieldValueInDynamicMessageBuilder(ballerinaBoolean);
    }

    public void setNullFieldValue(Object ballerinaNil) {
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + NIL, SERDES_ERROR);
    }

    public void setRecordFieldValue(BMap<BString, Object> ballerinaRecord) {
        Builder recordMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new RecordMessageSerializer(recordMessageBuilder, ballerinaRecord,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    public void setMapFieldValue(BMap<BString, Object> ballerinaMap) {
        Builder mapMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new MapMessageSerializer(mapMessageBuilder, ballerinaMap,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        Builder tableMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new TableMessageSerializer(tableMessageBuilder, ballerinaTable,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    public void setArrayFieldValue(BArray ballerinaArray) {
        Builder parentDynamicMessageBuilder = getDynamicMessageBuilder();
        // Wrap the parent dynamic message builder instead of passing a nested dynamic message builder
        MessageSerializer nestedMessageSerializer = new ArrayMessageSerializer(parentDynamicMessageBuilder,
                ballerinaArray, getBallerinaStructuredTypeMessageSerializer());
        nestedMessageSerializer.setCurrentFieldName(getCurrentFieldName());
        // This call adds the value to parent dynamic message builder
        getValueOfNestedMessage(nestedMessageSerializer);
    }

    public void setUnionFieldValue(Object unionValue) {
        Builder unionMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new UnionMessageSerializer(unionMessageBuilder, unionValue,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    public void setTupleFieldValue(BArray ballerinaTuple) {
        Builder tupleMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new TupleMessageSerializer(tupleMessageBuilder, ballerinaTuple,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    public Builder getDynamicMessageBuilderOfCurrentField() {
        FieldDescriptor fieldDescriptor = getCurrentFieldDescriptor();
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        return DynamicMessage.newBuilder(nestedSchema);
    }

    public DynamicMessage getValueOfNestedMessage(MessageSerializer childMessageSerializer) {
        MessageSerializer parentMessageSerializer = ballerinaStructuredTypeMessageSerializer.getMessageSerializer();
        // switch to child message serializer
        ballerinaStructuredTypeMessageSerializer.setMessageSerializer(childMessageSerializer);
        DynamicMessage nestedMessage = ballerinaStructuredTypeMessageSerializer.generateMessage().build();
        // switch back to parent message serializer
        ballerinaStructuredTypeMessageSerializer.setMessageSerializer(parentMessageSerializer);
        return nestedMessage;
    }

    public void setCurrentFieldValueInDynamicMessageBuilder(Object value) {
        FieldDescriptor currentFieldDescriptor = getCurrentFieldDescriptor();
        if (currentFieldDescriptor.isRepeated()) {
            getDynamicMessageBuilder().addRepeatedField(getCurrentFieldDescriptor(), value);
        } else {
            getDynamicMessageBuilder().setField(currentFieldDescriptor, value);
        }
    }

    public abstract List<MessageFieldData> getListOfMessageFieldData();
}
