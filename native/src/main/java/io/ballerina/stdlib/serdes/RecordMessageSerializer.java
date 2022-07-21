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
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link RecordMessageSerializer} class handles serialization of ballerina records.
 */
public class RecordMessageSerializer extends MessageSerializer {

    public RecordMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                   BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        @SuppressWarnings("unchecked") BMap<BString, Object> record = (BMap<BString, Object>) getAnydata();
        Map<String, Field> recordTypeFields = ((RecordType) record.getType()).getFields();
        return record.entrySet().stream().map(entry -> {
            String fieldName = entry.getKey().getValue();
            Object fieldValue = entry.getValue();
            Type fieldType = recordTypeFields.get(fieldName).getFieldType();
            Type referredType = TypeUtils.getReferredType(fieldType);
            return new MessageFieldData(fieldName, fieldValue, referredType);
        }).collect(Collectors.toList());
    }
}
