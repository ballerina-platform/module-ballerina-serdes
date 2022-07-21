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

import io.ballerina.runtime.api.TypeTags;
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

import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_MAP_AS_UNION_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.ARRAY_OF_TABLE_AS_UNION_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.MAP_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.RECORD_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TABLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_BUILDER_NAME;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link ArrayMessageType} class generate protobuf message definition for ballerina arrays.
 */
public class ArrayMessageType extends MessageType {
    private final MessageType parentMessageType;

    public ArrayMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        this(ballerinaType, messageBuilder, messageGenerator, null);
    }

    private ArrayMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                             BallerinaStructuredTypeMessageGenerator messageGenerator, MessageType parentMessageType) {
        super(ballerinaType, messageBuilder, messageGenerator);
        this.parentMessageType = parentMessageType;
    }

    public static ArrayMessageType withParentMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                                                         BallerinaStructuredTypeMessageGenerator messageGenerator,
                                                         MessageType parentMessageType) {
        return new ArrayMessageType(ballerinaType, messageBuilder, messageGenerator, parentMessageType);
    }


    @Override
    public void setCurrentFieldName(String fieldName) {
        // An array builder can have utmost one field
        if (super.getCurrentFieldName() == null) {
            super.setCurrentFieldName(fieldName);
        }
    }

    @Override
    public void setIntField(IntegerType integerType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(integerType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setByteField(ByteType byteType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(TypeTags.BYTE_TAG);
        // Use optional label instead of repeated label, protobuf supports bytes instead of byte
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, protoType);
    }

    @Override
    public void setFloatField(FloatType floatType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(floatType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setDecimalField(DecimalType decimalType) {
        ProtobufMessageBuilder decimalMessageDefinition = generateDecimalMessageDefinition();
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        messageBuilder.addNestedMessage(decimalMessageDefinition);
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(decimalType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setStringField(StringType stringType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(stringType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setBooleanField(BooleanType booleanType) {
        String protoType = DataTypeMapper.mapBallerinaTypeToProtoType(booleanType.getTag());
        addMessageFieldInMessageBuilder(REPEATED_LABEL, protoType);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        String childMessageName = getNestedRecordMessageName(recordType);
        addChildMessageDefinitionInMessageBuilder(childMessageName, recordType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedRecordMessageName(RecordType recordType) {
        boolean isAnonymousNestedRecord = isAnonymousBallerinaRecord(recordType);
        // if not a referenced recordType use "RecordBuilder" as message name
        String childMessageName = isAnonymousNestedRecord ? RECORD_BUILDER : recordType.getName();
        if (isAnonymousNestedRecord && isParentMessageIsRecordMessageOrTupleMessage()) {
            childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + childMessageName;
        }
        return childMessageName;
    }

    @Override
    public void setMapField(MapType mapType) {
        String childMessageName = getNestedMapMessageName();
        addChildMessageDefinitionInMessageBuilder(childMessageName, mapType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedMapMessageName() {
        String childMessageName = MAP_BUILDER;
        if (isParentMessageIsUnionMessage()) {
            // TODO: support array of map as union member
            throw createSerdesError(ARRAY_OF_MAP_AS_UNION_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        } else if (isParentMessageIsRecordMessageOrTupleMessage()) {
            childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + childMessageName;
        }
        return childMessageName;
    }

    @Override
    public void setTableField(TableType tableType) {
        String childMessageName = getNestedTableMessageName();
        addChildMessageDefinitionInMessageBuilder(childMessageName, tableType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedTableMessageName() {
        String childMessageName = TABLE_BUILDER;
        if (isParentMessageIsUnionMessage()) {
            // TODO: support array of table union member
            throw createSerdesError(ARRAY_OF_TABLE_AS_UNION_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        } else if (isParentMessageIsRecordMessageOrTupleMessage()) {
            childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + childMessageName;
        }
        return childMessageName;
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        String childMessageName = getNestedArrayMessageName();
        addChildMessageDefinitionInMessageBuilder(childMessageName, arrayType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedArrayMessageName() {
        String childMessageName = ARRAY_BUILDER_NAME;
        if (isParentMessageIsUnionMessage()) {
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());
            String ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            childMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            childMessageName = ballerinaType + TYPE_SEPARATOR + childMessageName;
        } else if (isParentMessageIsRecordMessageOrTupleMessage()) {
            int dimension = Utils.getArrayDimensions((ArrayType) getBallerinaType());
            childMessageName = ARRAY_BUILDER_NAME + SEPARATOR + (dimension - 1);
            Type ballerinaType = Utils.getBaseElementTypeOfBallerinaArray((ArrayType) getBallerinaType());
            if (ballerinaType.getTag() == TypeTags.MAP_TAG || ballerinaType.getTag() == TypeTags.TABLE_TAG
                    || isAnonymousBallerinaRecord(ballerinaType)) {
                childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + childMessageName;
            } else {
                childMessageName = ballerinaType.getName() + TYPE_SEPARATOR + childMessageName;
            }
        }
        return childMessageName;
    }

    @Override
    public void setUnionField(UnionType unionType) {
        String childMessageName = getNestedUnionMessageName();
        addChildMessageDefinitionInMessageBuilder(childMessageName, unionType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedUnionMessageName() {
        String childMessageName = UNION_BUILDER_NAME;
        if (isParentMessageIsUnionMessage() || isParentMessageIsRecordMessageOrTupleMessage()) {
            String ballerinaType = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) getBallerinaType());
            childMessageName = ballerinaType + TYPE_SEPARATOR + childMessageName;
        }
        return childMessageName;
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String childMessageName = getNestedTupleMessageName(tupleType);
        addChildMessageDefinitionInMessageBuilder(childMessageName, tupleType);
        addMessageFieldInMessageBuilder(REPEATED_LABEL, childMessageName);
    }

    private String getNestedTupleMessageName(TupleType tupleType) {
        String childMessageName = TUPLE_BUILDER;
        if (isParentMessageIsUnionMessage()) {
            String ballerinaType = tupleType.getName();
            if (ballerinaType.equals(EMPTY_STRING)) {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(tupleType), SERDES_ERROR);
            }
            childMessageName = ballerinaType + TYPE_SEPARATOR + TUPLE_BUILDER;
        } else if (isParentMessageIsRecordMessageOrTupleMessage()) {
            childMessageName = getCurrentFieldName() + TYPE_SEPARATOR + childMessageName;
        }
        return childMessageName;
    }

    private boolean isParentMessageIsUnionMessage() {
        return parentMessageType instanceof UnionMessageType;
    }

    private boolean isParentMessageIsRecordMessageOrTupleMessage() {
        return parentMessageType instanceof RecordMessageType || parentMessageType instanceof TupleMessageType;
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        ArrayType arrayType = (ArrayType) getBallerinaType();
        Type elementType = arrayType.getElementType();
        Type referredType = TypeUtils.getReferredType(elementType);
        return List.of(Map.entry(ARRAY_FIELD_NAME, referredType));
    }
}
