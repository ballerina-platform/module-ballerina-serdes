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

package io.ballerina.stdlib.serdes;

import io.ballerina.runtime.api.TypeTags;

import java.util.HashMap;
import java.util.Map;

/**
* Mapper class to map Java and Ballerina data types to Proto3 field types.
*
*/
public class DataTypeMapper {
    private static Map<String, String> javaTypeToProto;
    private static Map<Integer, String> ballerinaTypeTagToProto;

    public static String getProtoTypeFromJavaType(String type) {
        return javaTypeToProto.get(type);
    }
    public static String getProtoTypeFromTag(int tag) {
        return ballerinaTypeTagToProto.get(tag);
    }

    static {
        javaTypeToProto = new HashMap<>();
        javaTypeToProto.put("Double", "double");
        javaTypeToProto.put("Float", "double");
        javaTypeToProto.put("DecimalValue", "double");
        javaTypeToProto.put("Integer", "sint64");
        javaTypeToProto.put("Long", "sint64");
        javaTypeToProto.put("Boolean", "bool");
        javaTypeToProto.put("String", "string");
        javaTypeToProto.put("BmpStringValue", "string");
        javaTypeToProto.put("Byte", "bytes");
    }

    static {
        ballerinaTypeTagToProto = new HashMap<>();
        ballerinaTypeTagToProto.put(TypeTags.INT_TAG, "sint64");
        ballerinaTypeTagToProto.put(TypeTags.BYTE_TAG, "bytes");
        ballerinaTypeTagToProto.put(TypeTags.FLOAT_TAG, "double");
        ballerinaTypeTagToProto.put(TypeTags.DECIMAL_TAG, "double");
        ballerinaTypeTagToProto.put(TypeTags.STRING_TAG, "string");
        ballerinaTypeTagToProto.put(TypeTags.BOOLEAN_TAG, "bool");
    }
    
}
