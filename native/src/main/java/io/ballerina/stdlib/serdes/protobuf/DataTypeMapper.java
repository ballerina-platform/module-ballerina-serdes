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

    private static final Map<String, String> javaTypeToProto = new HashMap<>();
    private static final Map<Integer, String> ballerinaTypeTagToProto = new HashMap<>();
    private static final Map<String, String> javaTypeToBallerinaType = new HashMap<>();

    static {
        javaTypeToProto.put("Double", "double");
        javaTypeToProto.put("Float", "double");
        javaTypeToProto.put("DecimalValue", "DecimalValue");
        javaTypeToProto.put("Integer", "sint64");
        javaTypeToProto.put("Long", "sint64");
        javaTypeToProto.put("Boolean", "bool");
        javaTypeToProto.put("String", "string");
        javaTypeToProto.put("BmpStringValue", "string");
        javaTypeToProto.put("Byte", "bytes");

        javaTypeToBallerinaType.put("Double", "float");
        javaTypeToBallerinaType.put("Float", "float");
        javaTypeToBallerinaType.put("Short", "byte");
        javaTypeToBallerinaType.put("Integer", "byte");
        javaTypeToBallerinaType.put("Long", "int");
        javaTypeToBallerinaType.put("Boolean", "boolean");
        javaTypeToBallerinaType.put("String", "string");
        javaTypeToBallerinaType.put("BmpStringValue", "string");
        javaTypeToBallerinaType.put("Byte", "bytes");
        javaTypeToBallerinaType.put("DecimalValue", "decimal");

        ballerinaTypeTagToProto.put(TypeTags.INT_TAG, "sint64");
        ballerinaTypeTagToProto.put(TypeTags.BYTE_TAG, "bytes");
        ballerinaTypeTagToProto.put(TypeTags.FLOAT_TAG, "double");
        ballerinaTypeTagToProto.put(TypeTags.DECIMAL_TAG, "DecimalValue");
        ballerinaTypeTagToProto.put(TypeTags.STRING_TAG, "string");
        ballerinaTypeTagToProto.put(TypeTags.BOOLEAN_TAG, "bool");
    }

    public static String mapBallerinaTypeToProtoType(int ballerinaTypeTag) {
        return ballerinaTypeTagToProto.get(ballerinaTypeTag);
    }

    public static boolean isValidJavaType(String javaType) {
        return javaTypeToProto.containsKey(javaType);
    }

    public static String mapJavaTypeToBallerinaType(String javaType) {
        return javaTypeToBallerinaType.get(javaType);
    }
}
