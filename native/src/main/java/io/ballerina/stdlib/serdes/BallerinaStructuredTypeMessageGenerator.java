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
import io.ballerina.runtime.api.types.FiniteType;
import io.ballerina.runtime.api.types.FloatType;
import io.ballerina.runtime.api.types.IntegerType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.NullType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.StringType;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.stdlib.serdes.protobuf.ProtobufMessageBuilder;

import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * {@link BallerinaStructuredTypeMessageGenerator} generate protobuf message definition for given ballerina structure
 * type.
 */
public class BallerinaStructuredTypeMessageGenerator {
    private MessageType messageType;

    public BallerinaStructuredTypeMessageGenerator(Type type, ProtobufMessageBuilder messageBuilder) {
        switch (type.getTag()) {
            case TypeTags.RECORD_TYPE_TAG:
                setMessageType(new RecordMessageType(type, messageBuilder, this));
                break;
            case TypeTags.ARRAY_TAG:
                setMessageType(new ArrayMessageType(type, messageBuilder, this));
                break;
            case TypeTags.TUPLE_TAG:
                setMessageType(new TupleMessageType(type, messageBuilder, this));
                break;
            case TypeTags.UNION_TAG:
                setMessageType(new UnionMessageType(type, messageBuilder, this));
                break;
            case TypeTags.MAP_TAG:
                setMessageType(new MapMessageType(type, messageBuilder, this));
                break;
            case TypeTags.TABLE_TAG:
                setMessageType(new TableMessageType(type, messageBuilder, this));
                break;
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public ProtobufMessageBuilder generateMessageDefinition() {
        List<Map.Entry<String, Type>> fieldNamesAndTypes = messageType.getFiledNameAndBallerinaTypeEntryList();

        for (Map.Entry<String, Type> entry : fieldNamesAndTypes) {
            String fieldEntryName = entry.getKey();
            Type fieldEntryType = entry.getValue();

            messageType.setCurrentFieldName(fieldEntryName);

            switch (fieldEntryType.getTag()) {
                case TypeTags.NULL_TAG:
                    messageType.setNullField((NullType) fieldEntryType);
                    break;
                case TypeTags.INT_TAG:
                    messageType.setIntField((IntegerType) fieldEntryType);
                    break;
                case TypeTags.BYTE_TAG:
                    messageType.setByteField((ByteType) fieldEntryType);
                    break;
                case TypeTags.FLOAT_TAG:
                    messageType.setFloatField((FloatType) fieldEntryType);
                    break;
                case TypeTags.STRING_TAG:
                    messageType.setStringField((StringType) fieldEntryType);
                    break;
                case TypeTags.BOOLEAN_TAG:
                    messageType.setBooleanField((BooleanType) fieldEntryType);
                    break;
                case TypeTags.DECIMAL_TAG:
                    messageType.setDecimalField((DecimalType) fieldEntryType);
                    break;
                case TypeTags.FINITE_TYPE_TAG:
                    messageType.setEnumField((FiniteType) fieldEntryType);
                    break;
                case TypeTags.UNION_TAG:
                    messageType.setUnionField((UnionType) fieldEntryType);
                    break;
                case TypeTags.ARRAY_TAG:
                    messageType.setArrayField((ArrayType) fieldEntryType);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    messageType.setRecordField((RecordType) fieldEntryType);
                    break;
                case TypeTags.TUPLE_TAG:
                    messageType.setTupleField((TupleType) fieldEntryType);
                    break;
                case TypeTags.TABLE_TAG:
                    messageType.setTableField((TableType) fieldEntryType);
                    break;
                case TypeTags.MAP_TAG:
                    messageType.setMapField((MapType) fieldEntryType);
                    break;
                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + fieldEntryType.getName(), SERDES_ERROR);
            }
            messageType.incrementFieldNumber();
        }
        return messageType.getMessageBuilder();
    }
}
