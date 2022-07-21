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
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.serdes.protobuf.DataTypeMapper;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.BOOL;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.MAP_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.NIL;
import static io.ballerina.stdlib.serdes.Constants.NULL_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.OPTIONAL_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.STRING;
import static io.ballerina.stdlib.serdes.Constants.TABLE_MEMBER_NOT_YET_SUPPORTED;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_BUILDER;
import static io.ballerina.stdlib.serdes.Constants.TYPE_SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.UNION_FIELD_NAME;
import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;
import static io.ballerina.stdlib.serdes.Utils.isAnonymousBallerinaRecord;

/**
 * {@link UnionMessageType} class generate protobuf message definition for ballerina union.
 */
public class UnionMessageType extends MessageType {
    public UnionMessageType(Type ballerinaType, ProtobufMessageBuilder messageBuilder,
                            BallerinaStructuredTypeMessageGenerator messageGenerator) {
        super(ballerinaType, messageBuilder, messageGenerator);
    }

    public static Map.Entry<String, Type> mapMemberToFieldName(Type memberType) {
        Type referredType = TypeUtils.getReferredType(memberType);
        String typeName = referredType.getName();

        if (referredType.getTag() == TypeTags.ARRAY_TAG) {
            int dimention = Utils.getArrayDimensions((ArrayType) referredType);
            typeName = Utils.getBaseElementTypeNameOfBallerinaArray((ArrayType) referredType);
            String key = typeName + TYPE_SEPARATOR + ARRAY_FIELD_NAME + SEPARATOR + dimention + TYPE_SEPARATOR
                    + UNION_FIELD_NAME;
            return Map.entry(key, referredType);
        }

        // Handle enum members
        if (referredType.getTag() == TypeTags.FINITE_TYPE_TAG
                && TypeUtils.getType(referredType.getEmptyValue()).getTag() == TypeTags.STRING_TAG) {
            return Map.entry(((BString) referredType.getEmptyValue()).getValue(), referredType);
        }

        if (DataTypeMapper.isValidBallerinaPrimitiveType(typeName)
                || referredType.getTag() == TypeTags.RECORD_TYPE_TAG) {
            String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
            return Map.entry(key, referredType);
        }

        if (typeName.equals(NIL)) {
            return Map.entry(NULL_FIELD_NAME, referredType);
        }

        if (referredType.getTag() == TypeTags.TUPLE_TAG) {
            if (!typeName.equals(EMPTY_STRING)) {
                String key = typeName + TYPE_SEPARATOR + UNION_FIELD_NAME;
                return Map.entry(key, referredType);
            } else {
                throw createSerdesError(Utils.typeNotSupportedErrorMessage(memberType), SERDES_ERROR);
            }
        }

        if (referredType.getTag() == TypeTags.MAP_TAG) {
            // TODO: support map member
            throw createSerdesError(MAP_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        if (referredType.getTag() == TypeTags.TABLE_TAG) {
            // TODO: support table member
            throw createSerdesError(TABLE_MEMBER_NOT_YET_SUPPORTED, SERDES_ERROR);
        }

        throw createSerdesError(UNSUPPORTED_DATA_TYPE + referredType.getName(), SERDES_ERROR);
    }

    @Override
    public void setEnumField(FiniteType finiteType) {
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, STRING);
    }

    @Override
    public void setNullField(NullType nullType) {
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, BOOL);
    }

    @Override
    public void setRecordField(RecordType recordType) {
        if (isAnonymousBallerinaRecord(recordType)) {
            throw createSerdesError(Utils.typeNotSupportedErrorMessage(recordType), SERDES_ERROR);
        }
        String nestedMessageName = recordType.getName();
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, recordType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public void setMapField(MapType mapType) {
        // TODO: add support to map type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + mapType.getName(), SERDES_ERROR);
    }

    @Override
    public void setTableField(TableType tableType) {
        // TODO: add support to table type
        throw createSerdesError(UNSUPPORTED_DATA_TYPE + tableType.getName(), SERDES_ERROR);
    }

    @Override
    public void setArrayField(ArrayType arrayType) {
        ProtobufMessageBuilder messageBuilder = getMessageBuilder();
        MessageType parentMessageType = getMessageGenerator().getMessageType();

        // Wrap existing message builder instead of creating new nested message builder
        MessageType childMessageType = ArrayMessageType.withParentMessageType(arrayType, messageBuilder,
                getMessageGenerator(), parentMessageType);
        childMessageType.setCurrentFieldName(getCurrentFieldName());
        childMessageType.setCurrentFieldNumber(getCurrentFieldNumber());

        // This call adds the value field in wrapped messageBuilder
        getNestedMessageDefinition(childMessageType);
    }

    @Override
    public void setUnionField(UnionType unionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleField(TupleType tupleType) {
        String nestedMessageName = tupleType.getName() + TYPE_SEPARATOR + TUPLE_BUILDER;
        addChildMessageDefinitionInMessageBuilder(nestedMessageName, tupleType);
        addMessageFieldInMessageBuilder(OPTIONAL_LABEL, nestedMessageName);
    }

    @Override
    public List<Map.Entry<String, Type>> getFiledNameAndBallerinaTypeEntryList() {
        UnionType unionType = (UnionType) getBallerinaType();
        return unionType.getMemberTypes().stream().map(UnionMessageType::mapMemberToFieldName)
                .sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
    }
}
