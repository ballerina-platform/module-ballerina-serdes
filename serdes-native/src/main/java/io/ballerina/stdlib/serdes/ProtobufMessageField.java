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

import com.google.protobuf.DescriptorProtos;

import java.util.HashMap;
import java.util.Map;

/**
* Message Field Definitions.
*
*/
public class ProtobufMessageField {

    private static Map<String, DescriptorProtos.FieldDescriptorProto.Type> fieldTypes;
    private static Map<String, DescriptorProtos.FieldDescriptorProto.Label> fieldLabels;

    public static DescriptorProtos.FieldDescriptorProto.Type getFieldType(String type) {
        return fieldTypes.get(type);
    }

    public static DescriptorProtos.FieldDescriptorProto.Label getFieldLabel(String type) {
        return fieldLabels.get(type);
    }

    static {
        fieldTypes = new HashMap<>();
        fieldTypes.put("double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
        fieldTypes.put("float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT);
        fieldTypes.put("int32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        fieldTypes.put("int64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
        fieldTypes.put("uint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
        fieldTypes.put("uint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
        fieldTypes.put("sint32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
        fieldTypes.put("sint64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        fieldTypes.put("fixed32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
        fieldTypes.put("fixed64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
        fieldTypes.put("sfixed32", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
        fieldTypes.put("sfixed64", DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
        fieldTypes.put("bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
        fieldTypes.put("string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        fieldTypes.put("bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);

        fieldLabels = new HashMap<>();
        fieldLabels.put("optional", DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        fieldLabels.put("required", DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldLabels.put("repeated", DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
    }
}
