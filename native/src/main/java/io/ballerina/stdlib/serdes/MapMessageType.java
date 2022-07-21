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

import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.BooleanType;
import io.ballerina.runtime.api.types.ByteType;
import io.ballerina.runtime.api.types.DecimalType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageFieldBuilder;

import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.KEY_NAME;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD;
import static io.ballerina.stdlib.serdes.Constants.MAP_FIELD_ENTRY;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.VALUE_NAME;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link MapMessageType} class generate protobuf message definition for ballerina maps.
 */
public class MapMessageType extends MessageType {
    private static final int keyFieldNumber = 1;
    private static final int valueFieldNumber = 2;
    private final ProtobufMessageBuilder mapEntryBuilder;

    public MapMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                          BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
        mapEntryBuilder = new ProtobufMessageBuilder(MAP_FIELD_ENTRY);
        addKeyFieldInMapEntryBuilder();
    }

    @Override
    public void setIntField(IntegerType integerType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(integerType.getTag());
        addValueFieldInMapEntryBuilder(protoType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(byteType.getTag());
        addValueFieldInMapEntryBuilder(protoType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setFloatField(FloatType floatType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(floatType.getTag());
        addValueFieldInMapEntryBuilder(protoType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder decimalMessageDefinition = generateDecimalMessageDefinition();
        addValueFieldInMapEntryBuilder(decimalMessageDefinition);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setStringField(StringType stringType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(stringType.getTag());
        addValueFieldInMapEntryBuilder(protoType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(booleanType.getTag());
        addValueFieldInMapEntryBuilder(protoType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String childMessageName = isAnonymousBallerinaRecord(recordType) ? RECORD_BUILDER : recordType.getName();
        boolean hasMessageDefinition = mapEntryBuilder.hasMessageDefinitionInMessageTree(childMessageName);
        // Avoid recursive message definition for ballerina record with cyclic reference
        if (!hasMessageDefinition) {
            ProtobufMessageBuilder valueMessageBuilder = new ProtobufMessageBuilder(childMessageName, mapEntryBuilder);
            MessageType childMessageType = new RecordMessageType(recordType, valueMessageBuilder,
                    getMessageGenerator());
            ProtobufMessageBuilder nestedMessageDefinition = getNestedMessageDefinition(childMessageType);
            mapEntryBuilder.addNestedMessage(nestedMessageDefinition);
        }
        addValueFieldInMapEntryBuilder(childMessageName);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setMapField(MapType mapType) {
        ProtobufMessageBuilder valueMessageBuilder = new ProtobufMessageBuilder(MAP_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new MapMessageType(mapType, valueMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder valueMessageDefinition = getNestedMessageDefinition(childMessageType);
        addValueFieldInMapEntryBuilder(valueMessageDefinition);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setTableField(TableType tableType) {
        ProtobufMessageBuilder valueMessageBuilder = new ProtobufMessageBuilder(TABLE_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new TableMessageType(tableType, valueMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder valueMessageDefinition = getNestedMessageDefinition(childMessageType);
        addValueFieldInMapEntryBuilder(valueMessageDefinition);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap mapEntryBuilder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, mapEntryBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(valueFieldNumber);

        // This call adds the value field in wrapped mapEntryBuilder
        getNestedMessageDefinition(childMessageType);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String childMessageName = VALUE_NAME + TYPE_SEPARATOR + UNION_BUILDER_NAME;
        ProtobufMessageBuilder valueMessageBuilder = new ProtobufMessageBuilder(childMessageName, mapEntryBuilder);
        MessageType childMessageType = new UnionMessageType(unionType, valueMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder valueMessageDefinition = getNestedMessageDefinition(childMessageType);
        addValueFieldInMapEntryBuilder(valueMessageDefinition);
        addMapEntryFieldInMessageBuilder();
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        ProtobufMessageBuilder valueMessageBuilder = new ProtobufMessageBuilder(TUPLE_BUILDER, mapEntryBuilder);
        MessageType childMessageType = new TupleMessageType(tupleType, valueMessageBuilder, getMessageGenerator());
        ProtobufMessageBuilder valueMessageDefinition = getNestedMessageDefinition(childMessageType);
        addValueFieldInMapEntryBuilder(valueMessageDefinition);
        addMapEntryFieldInMessageBuilder();
    }

    private void addValueFieldInMapEntryBuilder(ProtobufMessageBuilder valueMessageDefinition) {
        mapEntryBuilder.addNestedMessage(valueMessageDefinition);
        addValueFieldInMapEntryBuilder(valueMessageDefinition.getName());
    }

    private void addValueFieldInMapEntryBuilder(String fieldType) {
        ProtobufMessageFieldBuilder valueField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, fieldType,
                getCurrentFieldName(), valueFieldNumber);
        mapEntryBuilder.addField(valueField);
    }

    private void addKeyFieldInMapEntryBuilder() {
        ProtobufMessageFieldBuilder keyField = new ProtobufMessageFieldBuilder(OPTIONAL_LABEL, STRING, KEY_NAME,
                keyFieldNumber);
        mapEntryBuilder.addField(keyField);
    }

    void addMapEntryFieldInMessageBuilder() {
        final int mapEntryNumber = 1;
        // add map entry builder definition
        getMessageBuilder().addNestedMessage(mapEntryBuilder);
        ProtobufMessageFieldBuilder mapEntryField = new ProtobufMessageFieldBuilder(REPEATED_LABEL,
                mapEntryBuilder.getName(), MAP_FIELD, mapEntryNumber);
        getMessageBuilder().addField(mapEntryField);
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        MapType mapType = (MapType) getBallerinaType();
        return List.of(Map.entry(VALUE_NAME, TypeUtils.getReferredType(mapType.getConstrainedType())));
    }

}
