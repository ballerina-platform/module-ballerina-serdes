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
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;

import java.util.ArrayList;
import java.util.List;

import static io.ballerina.stdlib.serdes.Constants.ARRAY_FIELD_NAME;

/**
 * {@link ArrayMessageSerializer} class handles serialization of ballerina arrays.
 */
public class ArrayMessageSerializer extends MessageSerializer {

    public ArrayMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setCurrentFieldName(String fieldName) {
        if (super.getCurrentFieldName() == null) {
            super.setCurrentFieldName(fieldName);
        }
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        // Discard ballerinaByte parameter and set the entire byte array without repeating the field
        setCurrentFieldValueInDynamicMessageBuilder(((BArray) getAnydata()).getBytes());
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        Builder arrayMessageBuilder = getDynamicMessageBuilderOfCurrentField();
        MessageSerializer nestedMessageSerializer = new ArrayMessageSerializer(arrayMessageBuilder, ballerinaArray,
                getBallerinaStructuredTypeMessageSerializer());
        DynamicMessage nestedMessage = getValueOfNestedMessage(nestedMessageSerializer);
        setCurrentFieldValueInDynamicMessageBuilder(nestedMessage);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        BArray array = (BArray) getAnydata();
        Type referredType = TypeUtils.getReferredType(array.getElementType());
        int arraySize = array.size();
        List<MessageFieldData> messageFieldDataOfArrayElements = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            messageFieldDataOfArrayElements.add(new MessageFieldData(ARRAY_FIELD_NAME, array.get(i), referredType));
        }
        return messageFieldDataOfArrayElements;
    }
}
