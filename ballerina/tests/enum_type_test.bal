// Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

enum Color {
    RED="red",
    GREEN,
    BLUE
}

const OPEN = "open";
const CLOSE = "close";
type State OPEN|CLOSE;
type OptionalState OPEN|CLOSE?;

type RecordWithEnum record {
    Color color;
};

type MapWithEnum map<Color>;

@test:Config {}
public isolated function testBasicEnumType() returns error? {
    Color data = RED;
    Proto3Schema ser = check new (Color);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Color);
    Color decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testBasicEnumType2()returns error? {
    State data = OPEN;
    Proto3Schema ser = check new (State);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (State);
    State decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testRecordWithEnumField()returns error? {
    RecordWithEnum data = {color: RED};
    Proto3Schema ser = check new (RecordWithEnum);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (RecordWithEnum);
    RecordWithEnum decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testMapWithEnumConstraint()returns error? {
    MapWithEnum data = {"color": RED};
    Proto3Schema ser = check new (MapWithEnum);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (MapWithEnum);
    MapWithEnum decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testEnumWithOptional()returns error? {
    OptionalState data = ();
    Proto3Schema ser = check new (OptionalState);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (OptionalState);
    OptionalState decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}
