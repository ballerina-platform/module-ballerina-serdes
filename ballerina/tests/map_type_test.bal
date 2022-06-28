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

type MapInt map<int>;
type MapFloat map<float>;
type MapString map<string>;
type MapByte map<byte>;
type MapDecimal map<decimal>;
type MapBoolean map<boolean>;

type IntMatrix int[][];
type MapArray map<IntMatrix>;

type Status record {
    int code;
    string message?;
};

type MapRecord map<Status>;
type MapUnion map<Status|IntMatrix>;
type MapOfMaps map<MapUnion>;

type RecordWithMapField record {
    AgeMap ages;
};

type MapWithTuple map<TupleWithUnion>;

@test:Config{}
public isolated function testMapInt() returns error? {

    MapInt moduleLevel = {
        "serdes" : 1,
        "io" : 1,
        "http" : 5,
        "grpc" : 6
    };

    Proto3Schema ser = check new(MapInt);
    byte[] encode = check ser.serialize(moduleLevel);

    Proto3Schema des = check new(MapInt);
    MapInt decode = check des.deserialize(encode);

    test:assertEquals(decode, moduleLevel);
}


@test:Config{}
public isolated function testMapFloat() returns error? {

    MapFloat coord = {
        "x" : 1.5,
        "y" : 2.78,
        "z" : 5.78
    };

    Proto3Schema ser = check new(MapFloat);
    byte[] encode = check ser.serialize(coord);

    Proto3Schema des = check new(MapFloat);
    MapFloat decode = check des.deserialize(encode);

    test:assertEquals(decode, coord);
}

@test:Config{}
public isolated function testMapString() returns error? {

    MapString module = {
        "org" : "ballerina",
        "module" : "serdes"
    };

    Proto3Schema ser = check new(MapString);
    byte[] encode = check ser.serialize(module);

    Proto3Schema des = check new(MapString);
    MapString decode = check des.deserialize(encode);

    test:assertEquals(decode, module);
}

@test:Config{}
public isolated function testMapByte() returns error? {

    MapByte 'version = {
        "major" : 2,
        "minor" : 1,
        "patch" : 0
    };

    Proto3Schema ser = check new(MapByte);
    byte[] encode = check ser.serialize('version);

    Proto3Schema des = check new(MapByte);
    MapByte decode = check des.deserialize(encode);

    test:assertEquals(decode, 'version);
}

@test:Config{}
public isolated function testMapDecimal() returns error? {

    MapDecimal exchangeRate = {
        "LK" : 0.0028d,
        "USD" : 1d,
        "INR" : 0.013d
    };

    Proto3Schema ser = check new(MapDecimal);
    byte[] encode = check ser.serialize(exchangeRate);

    Proto3Schema des = check new(MapDecimal);
    MapDecimal decode = check des.deserialize(encode);

    test:assertEquals(decode, exchangeRate);
}

@test:Config{}
public isolated function testMapBoolean() returns error? {

    MapBoolean data = {
        "adult" : true,
        "married" : false
    };

    Proto3Schema ser = check new(MapBoolean);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapBoolean);
    MapBoolean decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapArray() returns error? {

    MapArray data = {
        "age groups" : [[0,10],[11,20],[21,30],[31, 60]]
    };

    Proto3Schema ser = check new(MapArray);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapArray);
    MapArray decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapRecord() returns error? {

    MapRecord data = {
        "404" : {code: 404, message: "Not found"},
        "405" : {code: 405, message: "Method Not Allowed"}
    };

    Proto3Schema ser = check new(MapRecord);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapRecord);
    MapRecord decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapUnion() returns error? {

    MapUnion data = {
        "404" : {code: 404, message: "Not found"},
        "age groups" : [[0,10],[11,20],[21,30],[31, 60]]
    };

    Proto3Schema ser = check new(MapUnion);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapUnion);
    MapUnion decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapOfMaps() returns error? {

    MapOfMaps data = {
        "status": {
            "404": {code: 404, message: "Not found"},
            "405" : {code: 405, message: "Method Not Allowed"}
        },
        "matrix": {
            "age groups": [[0, 10], [11, 20], [21, 30], [31, 60]]
        }
    };

    Proto3Schema ser = check new(MapOfMaps);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapOfMaps);
    MapOfMaps decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapFieldinRecord() returns error? {

    RecordWithMapField data = {
        ages: {"Tony Hoare": 88}
    };

    Proto3Schema ser = check new(RecordWithMapField);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(RecordWithMapField);
    RecordWithMapField decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

@test:Config{}
public isolated function testMapWithTupleElement() returns error? {
    MapWithTuple data = {
       "first": ["serdes", 1.2],
       "second": ["module", 2.4]
    };

    Proto3Schema ser = check new(MapWithTuple);
    byte[] encode = check ser.serialize(data);

    Proto3Schema des = check new(MapWithTuple);
    MapWithTuple decode = check des.deserialize(encode);

    test:assertEquals(decode, data);
}

