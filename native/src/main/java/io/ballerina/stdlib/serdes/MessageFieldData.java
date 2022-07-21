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

import io.ballerina.runtime.api.types.Type;

/**
 * {@link MessageFieldData} holds the ballerina value, ballerina type and generated field name of a protobuf field.
 */
public class MessageFieldData {
    private final String fieldName;
    private final Object ballerinaValue;
    private final Type ballerinaType;

    public MessageFieldData(String fieldName, Object ballerinaValue, Type ballerinaType) {
        this.fieldName = fieldName;
        this.ballerinaValue = ballerinaValue;
        this.ballerinaType = ballerinaType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Object getBallerinaValue() {
        return ballerinaValue;
    }

    public Type getBallerinaType() {
        return ballerinaType;
    }
}
