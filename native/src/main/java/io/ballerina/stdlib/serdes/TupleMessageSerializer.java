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
import io.ballerina.runtime.api.types.TupleType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;

import java.util.ArrayList;
import java.util.List;

import static io.ballerina.stdlib.serdes.Constants.SEPARATOR;
import static io.ballerina.stdlib.serdes.Constants.TUPLE_FIELD_NAME;

/**
 * {@link TupleMessageSerializer} class handles serialization of ballerina tuples.
 */
public class TupleMessageSerializer extends MessageSerializer {


    public TupleMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        BArray tuple = (BArray) getAnydata();
        List<MessageFieldData> messageFieldDataOfTupleElements = new ArrayList<>();
        List<Type> elementTypes = ((TupleType) tuple.getType()).getTupleTypes();
        for (int i = 0; i < tuple.size(); i++) {
            Object elementData = tuple.get(i);
            Type elementType = elementTypes.get(i);
            String fieldNameForElement = TUPLE_FIELD_NAME + SEPARATOR + (i + 1);
            messageFieldDataOfTupleElements.add(new MessageFieldData(fieldNameForElement, elementData, elementType));
        }
        return messageFieldDataOfTupleElements;
    }
}
