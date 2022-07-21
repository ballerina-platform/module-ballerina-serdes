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
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FieldDescriptor;
import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;

/**
 * {@link MapMessageSerializer} class handles serialization of ballerina maps.
 */
public class MapMessageSerializer extends MessageSerializer {
    private final Builder mapEntryBuilder;

    public MapMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
        FieldDescriptor mapFieldDescriptor = getDynamicMessageBuilder().getDescriptorForType()
                .findFieldByName(MAP_FIELD);
        Descriptor mapEntryDescriptor = mapFieldDescriptor.getMessageType();
        mapEntryBuilder = DynamicMessage.newBuilder(mapEntryDescriptor);
    }

    @Override
    public void setIntFieldValue(Object ballerinaInt) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaInt);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        setKeyFieldValueInMapEntryBuilder();
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        byte[] byteValue = new byte[]{ballerinaByte.byteValue()};
        mapEntryBuilder.setField(fieldDescriptor, byteValue);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setFloatFieldValue(Object ballerinaFloat) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaFloat);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        setKeyFieldValueInMapEntryBuilder();
        Builder decimalMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        DynamicMessage decimalMessage = generateDecimalValueMessage(decimalMessageBuilder, ballerinaDecimal);
        setValueFieldValueInMapEntryBuilder(decimalMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaString.getValue());
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        setKeyFieldValueInMapEntryBuilder();
        setValueFieldValueInMapEntryBuilder(ballerinaBoolean);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setRecordFieldValue(BMap<BString, Object> ballerinaRecord) {
        setKeyFieldValueInMapEntryBuilder();
        Builder recordMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new RecordMessageSerializer(recordMessageBuilder, ballerinaRecord,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setValueFieldValueInMapEntryBuilder(nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setMapFieldValue(BMap<BString, Object> ballerinaMap) {
        setKeyFieldValueInMapEntryBuilder();
        Builder recordMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new MapMessageSerializer(recordMessageBuilder, ballerinaMap,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setValueFieldValueInMapEntryBuilder(nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        setKeyFieldValueInMapEntryBuilder();
        Builder tableMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new TableMessageSerializer(tableMessageBuilder, ballerinaTable,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setValueFieldValueInMapEntryBuilder(nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        setKeyFieldValueInMapEntryBuilder();
        // Wrap the mapEntryBuilder instead of passing a new nestedMessageBuilder
        MessageSerializer nestedMessageSerializer = new ArrayMessageSerializer(mapEntryBuilder, ballerinaArray,
                getBallerinaStructuredTypeMessageSerializer());
        nestedMessageSerializer.setCurrentFieldName(VALUE_NAME);

        // This call adds the value to parent dynamic message builder
        getValueOfNestedMessage(nestedMessageSerializer);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setUnionFieldValue(Object unionValue) {
        setKeyFieldValueInMapEntryBuilder();
        Builder unionMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new UnionMessageSerializer(unionMessageBuilder, unionValue,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setValueFieldValueInMapEntryBuilder(nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    @Override
    public void setTupleFieldValue(BArray ballerinaTuple) {
        setKeyFieldValueInMapEntryBuilder();
        Builder tupleMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new TupleMessageSerializer(tupleMessageBuilder, ballerinaTuple,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setValueFieldValueInMapEntryBuilder(nestedMessage);
        setMapFieldMessageValueInMessageBuilder();
    }

    public Builder getDynamicMessageBuilderOfCurrentField() {
        FieldDescriptor fieldDescriptor = mapEntryBuilder.getDescriptorForType().findFieldByName(VALUE_NAME);
        Descriptor nestedSchema = fieldDescriptor.getMessageType();
        return DynamicMessage.newBuilder(nestedSchema);
    }

    private void setKeyFieldValueInMapEntryBuilder() {
        Descriptor mapEntryDescriptor = mapEntryBuilder.getDescriptorForType();
        FieldDescriptor keyFieldDescriptor = mapEntryDescriptor.findFieldByName(KEY_NAME);
        String mapKeyFieldName = getCurrentFieldName();
        mapEntryBuilder.setField(keyFieldDescriptor, mapKeyFieldName);
    }

    private void setValueFieldValueInMapEntryBuilder(Object value) {
        Descriptor mapEntryDescriptor = mapEntryBuilder.getDescriptorForType();
        FieldDescriptor valueFieldDescriptor = mapEntryDescriptor.findFieldByName(VALUE_NAME);
        mapEntryBuilder.setField(valueFieldDescriptor, value);
    }

    private void setMapFieldMessageValueInMessageBuilder() {
        FieldDescriptor mapField = getDynamicMessageBuilder().getDescriptorForType().findFieldByName(MAP_FIELD);
        getDynamicMessageBuilder().addRepeatedField(mapField, mapEntryBuilder.build());
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        @SuppressWarnings("unchecked") BMap<BString, Object> ballerinaMap = (BMap<BString, Object>) getAnydata();
        MapType mapType = (MapType) ballerinaMap.getType();
        Type constrainedType = mapType.getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        return ballerinaMap.entrySet().stream()
                .map(entry -> new MessageFieldData(entry.getKey().getValue(), entry.getValue(),
                        referredConstrainedType)).collect(Collectors.toList());
    }
}
