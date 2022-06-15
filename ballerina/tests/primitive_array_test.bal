// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.

// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.import ballerina/test;

import ballerina/test;

type IntArray int[];
type ByteArray byte[];
type FloatArray float[];
type StringArray string[];
type BooleanArray boolean[];
type DecimalArray decimal[];

@test:Config {}
public isolated function testIntArray() returns error? {
    IntArray data = [1, 2, 3, 4, 5, 6];

    Proto3Schema ser = check new (IntArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (IntArray);
    IntArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testByteArray() returns error? {
    ByteArray data = base16 `aeeecdefabcd12345567888822`;

    Proto3Schema ser = check new (ByteArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (ByteArray);
    ByteArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testFloatArray() returns error? {
    FloatArray data = [1.00, 2, 3.5, 4, 5.99, 6];

    Proto3Schema ser = check new (FloatArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (FloatArray);
    FloatArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testStringArray() returns error? {
    StringArray data = ["1", "2", "3", "4", "5", "6"];

    Proto3Schema ser = check new (StringArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (StringArray);
    StringArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testBooleanArray() returns error? {
    BooleanArray data = [true, true, true, false, false, false];

    Proto3Schema ser = check new (BooleanArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (BooleanArray);
    BooleanArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testDecimalArray() returns error? {
    DecimalArray data = [1.2d, 5.6d, 8.9999999999999999d];

    Proto3Schema ser = check new (DecimalArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (DecimalArray);
    DecimalArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}
