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

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

/**
 * Constant variable for serdes related operations.
 */
public final class Constants {

    private Constants() {}

    public static final String EMPTY_STRING = "";

    // Constants related to ballerina type
    public static final String STRING = "string";
    public static final String NIL = "()";

    // Constants related to protobuf schema
    public static final String SCHEMA_NAME = "schema";
    public static final String UNION_BUILDER_NAME = "UnionBuilder";
    public static final String UNION_FIELD_NAME = "unionField";
    public static final String ARRAY_BUILDER_NAME = "ArrayBuilder";
    public static final String DECIMAL_VALUE = "DecimalValue";
    public static final String ARRAY_FIELD_NAME = "arrayField";
    public static final String ATOMIC_FIELD_NAME = "atomicField";
    public static final String NULL_FIELD_NAME = "nullField";
    public static final String VALUE_SUFFIX = "Value";
    public static final String SCALE = "scale";
    public static final String PRECISION = "precision";
    public static final String VALUE = "value";
    public static final String SYNTAX = "syntax";
    public static final String MAP_BUILDER = "MapBuilder";
    public static final String MAP_FIELD_ENTRY = "MapFieldEntry";
    public static final String MAP_FIELD = "mapField";
    public static final String KEY_NAME = "key";
    public static final String VALUE_NAME = "value";
    public static final String TABLE_BUILDER = "TableBuilder";
    public static final String TABLE_ENTRY = "tableEntry";
    public static final String TUPLE_BUILDER = "TupleBuilder";
    public static final String TUPLE_FIELD_NAME = "element";
    public static final String RECORD_BUILDER = "RecordBuilder";

    public static final String SEPARATOR = "_";
    public static final String TYPE_SEPARATOR = "___";
    public static final String SPACE = " ";
    public static final String CURLY_BRACE = "{";

    // Constants related to protobuf labels and types
    public static final String PROTO3 = "proto3";
    public static final String OPTIONAL_LABEL = "optional";
    public static final String REPEATED_LABEL = "repeated";
    public static final String BYTES = "bytes";
    public static final String UINT32 = "uint32";
    public static final String BOOL = "bool";

    // Constants related to error messages
    public static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
    public static final String DESERIALIZATION_ERROR_MESSAGE = "Failed to Deserialize data: ";
    public static final String SERIALIZATION_ERROR_MESSAGE = "Failed to Serialize data: ";
    public static final String TYPE_MISMATCH_ERROR_MESSAGE = "Type mismatch";
    public static final String SCHEMA_GENERATION_FAILURE = "Failed to generate schema: ";
    public static final String FAILED_WRITE_FILE = "Failed to write proto file: ";
    public static final String MAP_MEMBER_NOT_YET_SUPPORTED = "Serdes not yet support map type as union member";
    public static final String TABLE_MEMBER_NOT_YET_SUPPORTED = "Serdes not yet support table type as union member";
    public static final String ARRAY_OF_MAP_AS_UNION_MEMBER_NOT_YET_SUPPORTED = "Serdes not yet support array of maps"
            + " as union member";
    public static final String ARRAY_OF_TABLE_AS_UNION_MEMBER_NOT_YET_SUPPORTED = "Serdes not yet support array of"
            + " tables as union member";
    public static final BString BALLERINA_TYPEDESC_ATTRIBUTE_NAME = StringUtils.fromString("dataType");
}
