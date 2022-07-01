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
    RED,
    GREAN,
    BLUE
}

const OPEN = "open";
const CLOSE = "close";
type STATE OPEN|CLOSE;

type RecordWithEnum record {
    Color color;
};

type MapWithEnum map<Color>;

@test:Config {}
public isolated function testBasicEnumType()returns error? {
    Color data = RED;
    Proto3Schema ser = check new (Color);
    check ser.generateProtoFile("Color.proto");
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Color);
    Color decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testBasicEnumType2()returns error? {
    STATE data = OPEN;
    Proto3Schema ser = check new (STATE);
    check ser.generateProtoFile("STATE.proto");
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (STATE);
    STATE decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testRecordWithEnumField()returns error? {
    RecordWithEnum data = {color: RED};
    Proto3Schema ser = check new (RecordWithEnum);
    check ser.generateProtoFile("RecordWithEnum.proto");
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (RecordWithEnum);
    RecordWithEnum decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testMapWithEnumConstraint()returns error? {
    MapWithEnum data = {"color": RED};
    Proto3Schema ser = check new (MapWithEnum);
    check ser.generateProtoFile("MapWithEnum.proto");
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (MapWithEnum);
    MapWithEnum decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}
