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

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;

import static io.ballerina.stdlib.serdes.Constants.UNSUPPORTED_DATA_TYPE;
import static io.ballerina.stdlib.serdes.Utils.SERDES_ERROR;
import static io.ballerina.stdlib.serdes.Utils.createSerdesError;

/**
 * {@link BallerinaStructuredTypeMessageSerializer} generates dynamic message by setting field values.
 */
public class BallerinaStructuredTypeMessageSerializer {
    private MessageSerializer messageSerializer;

    public BallerinaStructuredTypeMessageSerializer(Type type, Object anydata, Builder dynamicMessageBuilder) {
        switch (type.getTag()) {
            case TypeTags.RECORD_TYPE_TAG:
                setMessageSerializer(new RecordMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            case TypeTags.UNION_TAG:
                setMessageSerializer(new UnionMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            case TypeTags.ARRAY_TAG:
                setMessageSerializer(new ArrayMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            case TypeTags.MAP_TAG:
                setMessageSerializer(new MapMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            case TypeTags.TABLE_TAG:
                setMessageSerializer(new TableMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            case TypeTags.TUPLE_TAG:
                setMessageSerializer(new TupleMessageSerializer(dynamicMessageBuilder, anydata, this));
                break;
            default:
                throw createSerdesError(UNSUPPORTED_DATA_TYPE + type.getName(), SERDES_ERROR);
        }
    }

    public MessageSerializer getMessageSerializer() {
        return messageSerializer;
    }

    public void setMessageSerializer(MessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
    }

    public Builder generateMessage() {
        List<MessageFieldData> fieldNamesAndValues = messageSerializer.getListOfMessageFieldData();

        for (MessageFieldData entry : fieldNamesAndValues) {
            Object ballerinaValue = entry.getBallerinaValue();
            Type ballerinaType = entry.getBallerinaType();
            String fieldName = entry.getFieldName();

            messageSerializer.setCurrentFieldName(fieldName);

            switch (ballerinaType.getTag()) {
                case TypeTags.NULL_TAG:
                    messageSerializer.setNullFieldValue(ballerinaValue);
                    break;
                case TypeTags.INT_TAG:
                    messageSerializer.setIntFieldValue(ballerinaValue);
                    break;
                case TypeTags.BYTE_TAG:
                    messageSerializer.setByteFieldValue((Integer) ballerinaValue);
                    break;
                case TypeTags.FLOAT_TAG:
                    messageSerializer.setFloatFieldValue(ballerinaValue);
                    break;
                case TypeTags.STRING_TAG:
                    messageSerializer.setStringFieldValue((BString) ballerinaValue);
                    break;
                case TypeTags.BOOLEAN_TAG:
                    messageSerializer.setBooleanFieldValue((Boolean) ballerinaValue);
                    break;
                case TypeTags.DECIMAL_TAG:
                    messageSerializer.setDecimalFieldValue((BDecimal) ballerinaValue);
                    break;
                case TypeTags.UNION_TAG:
                    messageSerializer.setUnionFieldValue(ballerinaValue);
                    break;
                case TypeTags.ARRAY_TAG:
                    messageSerializer.setArrayFieldValue((BArray) ballerinaValue);
                    break;
                case TypeTags.RECORD_TYPE_TAG:
                    @SuppressWarnings("unchecked") BMap<BString, Object> recordValue
                            = (BMap<BString, Object>) ballerinaValue;
                    messageSerializer.setRecordFieldValue(recordValue);
                    break;
                case TypeTags.TUPLE_TAG:
                    messageSerializer.setTupleFieldValue((BArray) ballerinaValue);
                    break;
                case TypeTags.TABLE_TAG:
                    messageSerializer.setTableFieldValue((BTable<?, ?>) ballerinaValue);
                    break;
                case TypeTags.MAP_TAG:
                    @SuppressWarnings("unchecked") BMap<BString, Object> mapValue
                            = (BMap<BString, Object>) ballerinaValue;
                    messageSerializer.setMapFieldValue(mapValue);
                    break;
                default:
                    throw createSerdesError(UNSUPPORTED_DATA_TYPE + ballerinaType.getName(), SERDES_ERROR);
            }
        }

        return messageSerializer.getDynamicMessageBuilder();
    }
}
