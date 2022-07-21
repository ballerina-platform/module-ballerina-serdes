/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.serdes.protobuf;

import io.ballerina.runtime.api.TypeTags;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides Java and Ballerina data types to Proto3 field types mapping.
 */
public class DataTypeMapper {

    private static final Map<Integer, String> ballerinaTypeTagToProto = new HashMap<>();
    private static final Map<String, String> ballerinaPrimitiveType = new HashMap<>();

    static {
        ballerinaTypeTagToProto.put(TypeTags.INT_TAG, "sint64");
        ballerinaTypeTagToProto.put(TypeTags.BYTE_TAG, "bytes");
        ballerinaTypeTagToProto.put(TypeTags.FLOAT_TAG, "double");
        ballerinaTypeTagToProto.put(TypeTags.DECIMAL_TAG, "DecimalValue");
        ballerinaTypeTagToProto.put(TypeTags.STRING_TAG, "string");
        ballerinaTypeTagToProto.put(TypeTags.BOOLEAN_TAG, "bool");

        ballerinaPrimitiveType.put("int", "int");
        ballerinaPrimitiveType.put("byte", "byte");
        ballerinaPrimitiveType.put("string", "string");
        ballerinaPrimitiveType.put("decimal", "decimal");
        ballerinaPrimitiveType.put("boolean", "boolean");
        ballerinaPrimitiveType.put("float", "float");
    }

    private DataTypeMapper() {
    }

    public static String mapBallerinaTypeToProtoType(int ballerinaTypeTag) {
        return ballerinaTypeTagToProto.get(ballerinaTypeTag);
    }

    public static boolean isValidBallerinaPrimitiveType(String typeName) {
        return ballerinaPrimitiveType.get(typeName) != null;
    }
}
