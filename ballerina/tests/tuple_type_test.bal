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
// under the License.

import ballerina/test;

type PrimitiveTuple [byte, int, float, boolean, string ,decimal];
type TupleWithUnion [byte|string, decimal|boolean];
type UnionTupleElement byte|string;
type TupleWithArray [string[], boolean[][], int[][][], UnionTupleElement[]];
type TupleWithRecord [Student, Teacher];
type TupleWithMap [map<int>, map<Student>];
type TupleWithTable [table<map<int>>, table<Student>];
type TupleOfTuples [PrimitiveTuple, TupleWithUnion];
type TupleWithTupleArrays [PrimitiveTuple[], TupleWithUnion[][]];
type TupleWithTableArrays [table<map<int>>[][], table<Student>[][]];
type TupleWithNonReferenceArrayOfTuple [[int, int][], [boolean, float][]];
type TupleWithNonReferenceRecords [record {string name;}, record {int id;}];
type TupleWithNonReferenceArrayOfRecords [record {string name;}[][][], record {int id;}[]];

@test:Config {}
public isolated function testTupleWithPrimitive() returns error? {
    PrimitiveTuple value = [254, 100000, 1.2, true, "serdes", 3.2e-5];

    Proto3Schema ser = check new (PrimitiveTuple);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (PrimitiveTuple);
    PrimitiveTuple decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithUnionElements() returns error? {
    TupleWithUnion value = ["serdes", 3.2e-5];

    Proto3Schema ser = check new (TupleWithUnion);
    byte[] encoded = check ser.serialize(value);
    
    Proto3Schema des = check new (TupleWithUnion);
    TupleWithUnion decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithArrayElements() returns error? {
    TupleWithArray value = [["serdes"], [[true, false], [false]], [[[1]]], ["serdes"]];

    Proto3Schema ser = check new (TupleWithArray);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithArray);
    TupleWithArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithRecordElements() returns error? {
    TupleWithRecord value = [{name: "Linus Torvalds", courseId: 123, fees: 3.4e10}, {name: "Andrew S. Tanenbaum", courseId: 123, salary: 3.4e10 * 10}];

    Proto3Schema ser = check new (TupleWithRecord);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithRecord);
    TupleWithRecord decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithMapElements() returns error? {
    TupleWithMap value = [
        {"a": 10, "b": 20, c: 30},
        {
            "Linux": {name: "Linus Torvalds", courseId: 123, fees: 3.4e10},
            "Minix": {name: "Andrew S. Tanenbaum", courseId: 123, fees: 3.4e10}
        }
    ];

    Proto3Schema ser = check new (TupleWithMap);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithMap);
    TupleWithMap decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithTableElements() returns error? {
    TupleWithTable value = [
        table [
                {"a": 10},
                {"b": 20, c: 30}
            ],
        table [
                {name: "Linus Torvalds", courseId: 123, fees: 3.4e10},
                {name: "Andrew S. Tanenbaum", courseId: 123, fees: 3.4e10}
            ]
    ];

    Proto3Schema ser = check new (TupleWithTable);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithTable);
    TupleWithTable decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithTupleElements() returns error? {
    TupleOfTuples value = [[254, 100000, 1.2, true, "serdes", 3.2e-5], ["serdes", 3.4e10]];

    Proto3Schema ser = check new (TupleOfTuples);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleOfTuples);
    TupleOfTuples decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithArrayOfTupleElements() returns error? {
    TupleWithTupleArrays value = [[[254, 100000, 1.2, true, "serdes", 3.2e-5]], [[["serdes", 3.4e10]]]];

    Proto3Schema ser = check new (TupleWithTupleArrays);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithTupleArrays);
    TupleWithTupleArrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithTableArrayElements() returns error? {
    TupleWithTableArrays value = [
        [[
            table [
                    {"a": 10},
                    {"b": 20, c: 30}
                ]
        ]],
        [[
            table [
                    {name: "Linus Torvalds", courseId: 123, fees: 3.4e10},
                    {name: "Andrew S. Tanenbaum", courseId: 123, fees: 3.4e10}
                ]
        ]]
    ];

    Proto3Schema ser = check new (TupleWithTableArrays);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithTableArrays);
    TupleWithTableArrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}

@test:Config {}
public isolated function testTupleWithNonReferenceArrayOfTuples() returns error? {
    TupleWithNonReferenceArrayOfTuple value = [[[2,3]], [[false, 2.4]]];

    Proto3Schema ser = check new (TupleWithNonReferenceArrayOfTuple);
    byte[] encoded = check ser.serialize(value);

    Proto3Schema des = check new (TupleWithNonReferenceArrayOfTuple);
    TupleWithNonReferenceArrayOfTuple decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, value);
}


@test:Config {}
public isolated function testTupleWithNonReferencedRecords() returns error? {
    TupleWithNonReferenceRecords data = [
        {name: "serdes"},
        {id: 1}
    ];

    Proto3Schema ser = check new (TupleWithNonReferenceRecords);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (TupleWithNonReferenceRecords);
    TupleWithNonReferenceRecords decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}


@test:Config {}
public isolated function testTupleWithNonReferencedArrayOfRecords() returns error? {
    TupleWithNonReferenceArrayOfRecords data = [
        [[[{name: "serdes"}]]],
        [{id: 1}]
    ];

    Proto3Schema ser = check new (TupleWithNonReferenceArrayOfRecords);

    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (TupleWithNonReferenceArrayOfRecords);
    TupleWithNonReferenceArrayOfRecords decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}
