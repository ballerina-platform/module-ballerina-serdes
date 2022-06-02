// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/test;

@test:Config {}
public isolated function testPrimitiveInt() returns error? {
    int value = 666;

    Proto3Schema ser = check new (int);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (int);
    int decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testPrimitiveBoolean() returns error? {
    boolean value = true;

    Proto3Schema ser = check new (boolean);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (boolean);
    boolean decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testPrimitiveFloat() returns error? {
    float value = 6.666;

    Proto3Schema ser = check new (float);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (float);
    float decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testPrimitiveDecimal() returns error? {
    decimal value = 1d / 100000d;

    Proto3Schema ser = check new (decimal);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (decimal);
    decimal decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testPrimitiveString() returns error? {
    string value = "module-ballerina-serdes";

    Proto3Schema ser = check new (string);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (string);
    string decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}
