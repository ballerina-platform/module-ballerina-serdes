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

type Student record {
    string name;
    int courseId;
    decimal fees;
};

type Teacher record {
    string name;
    int courseId;
    decimal salary;
};

type DecimalOrNil decimal?;
type PrimitiveUnion decimal|byte|int|float|boolean|string;
type UnionWithArrays int[][]|float[]|string[][][]|string[];
type UnionOfPrimitiveAndArrays decimal|byte|int|float|boolean|string|string[]|int[][]|float[][][][][]?;
type UnionWithRecords Student|Teacher;

type A byte[]|string;
type B ();
type C A|B;
type D A|boolean[];
type UnionOfUnionArray C[][]|D[];

type UnionMember record {
    string name;
    int id;
};

type UnionWithRecord int|string[]|UnionMember|();
type CompleUnion UnionOfPrimitiveAndArrays|UnionOfUnionArray|UnionWithRecord;
type UnionWithArrayOfMaps MapString[]|MapInt[];

type TupleA [int, string];
type TupleB [boolean, decimal];
type UnionOfTuples TupleA | TupleB;
type UnionOfTupleArrays TupleA[] | TupleB[];

@test:Config {}
public isolated function testPrimitiveUnion() returns error? {
    PrimitiveUnion nums = 3.9d;

    Proto3Schema ser = check new (PrimitiveUnion);
    byte[] encoded = check ser.serialize(nums);

    Proto3Schema des = check new (PrimitiveUnion);
    PrimitiveUnion decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nums);
}

@test:Config {}
public isolated function testFieldWithNil() returns error? {
    Proto3Schema ser = check new (DecimalOrNil);
    byte[] encoded = check ser.serialize(());

    Proto3Schema des = check new (DecimalOrNil);
    DecimalOrNil decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, ());
}

@test:Config {}
public isolated function testUnionWithArrays() returns error? {
    int[][] nums = [[1, 9, 2, 1, 6, 8],[1, 9, 2, 1, 6, 8]];

    Proto3Schema ser = check new (UnionWithArrays);
    byte[] encoded = check ser.serialize(nums);

    Proto3Schema des = check new (UnionWithArrays);
    int[][] decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nums);
}

@test:Config{}
public isolated function testUnionWithRecords() returns error? {
    Student student = {
        name: "Jack",
        courseId: 12001,
        fees: 20e4
    };

    Proto3Schema ser = check new (UnionWithRecords);
    byte[] encoded = check ser.serialize(student);

    Proto3Schema des = check new (UnionWithRecords);
    UnionWithRecords decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, student);
}

@test:Config {}
public isolated function testUnionWithPrimitivesAndArrays() returns error? {
    UnionOfPrimitiveAndArrays nums = 3.9d;

    Proto3Schema ser = check new (UnionOfPrimitiveAndArrays);
    byte[] encoded = check ser.serialize(nums);

    Proto3Schema des = check new (UnionOfPrimitiveAndArrays);
    UnionOfPrimitiveAndArrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nums);
}

@test:Config {}
public isolated function testUnionOfUnionArrays() returns error? {
    UnionOfUnionArray nums = [[[1],"sredes",()], []];

    Proto3Schema ser = check new (UnionOfUnionArray);
    byte[] encoded = check ser.serialize(nums);

    Proto3Schema des = check new (UnionOfUnionArray);
    UnionOfUnionArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nums);
}

@test:Config {}
public isolated function testUnionWithRecord() returns error? {
    UnionMember member = {name: "Jane", id: 100};

    Proto3Schema ser = check new (UnionWithRecord);
    byte[] encoded = check ser.serialize(member);

    Proto3Schema des = check new (UnionWithRecord);
    UnionWithRecord decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, member);
}

@test:Config {}
public isolated function testComplexUnion() returns error? {
    UnionMember member = {name: "Jane", id: 100};

    Proto3Schema ser = check new (CompleUnion);
    byte[] encoded = check ser.serialize(member);

    Proto3Schema des = check new (CompleUnion);
    CompleUnion decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, member);
}

@test:Config {}
public isolated function testUnionOfTuples() returns error? {
    UnionOfTuples data = [10, "serdes"];

    Proto3Schema ser = check new (UnionOfTuples);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (UnionOfTuples);
    UnionOfTuples decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testUnionOfTupleArrays() returns error? {
    UnionOfTupleArrays data = [[10, "serdes"]];

    Proto3Schema ser = check new (UnionOfTupleArrays);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (UnionOfTupleArrays);
    UnionOfTupleArrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}