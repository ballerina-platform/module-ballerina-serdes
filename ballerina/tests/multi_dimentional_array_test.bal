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
// under the License.import ballerina/test;

import ballerina/test;

type Int2DArray IntArray[];
type Int3DArray Int2DArray[];
type Int4DArray Int3DArray[];

type String2DArray StringArray[];
type String3DArray String2DArray[];
type String4DArray String3DArray[];

type Byte5DArray byte[][][][][];

type Decimal3DArray DecimalArray[][];

type TestMember record {
    string fullName;
    int id;
};

type UnionType int|string[]|TestMember[]|TestMember;
type Level3Array UnionType[];
type Level2Array Level3Array[];
// Complex array
type Level1Array Level2Array[];

type AgeMap map<int>;
type AgeMapArray AgeMap[];
type AgeMap2DArray AgeMapArray[];

@test:Config {}
public isolated function testInt2DArray() returns error? {
    Int2DArray data = [
        [1, 2],
        [3, 4],
        [5],
        [6]
    ];

    Proto3Schema ser = check new (Int2DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Int2DArray);
    Int2DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testInt3DArray() returns error? {
    Int3DArray data = [
        [[1, 2]],
        [[3, 4]],
        [[5]],
        [[6]]
    ];

    Proto3Schema ser = check new (Int3DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Int3DArray);
    Int3DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testString4DArray() returns error? {
    String4DArray data = [
        [
            [["1", "2"]],
            [["3", "4"], ["6"], ["5"]],
            [["1", "2"]],
            [["3", "4"], ["6"], ["5"]]
        ],
        [[["5"]]],
        [[["6"], ["5"]]]
    ];
    Proto3Schema ser = check new (String4DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (String4DArray);
    String4DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testInt4DArray() returns error? {
    Int4DArray data = [
        [
            [[1, 2]],
            [[3, 4], [6], [5]],
            [[1, 2]],
            [[3, 4], [6], [5]]
        ],
        [[[5]]],
        [[[6], [5]]]
    ];

    Proto3Schema ser = check new (Int4DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Int4DArray);
    Int4DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testByte5DArray() returns error? {
    Byte5DArray data = [
        [
            [[[1, 2]], [[3, 4], [6], [5]]],
            [
                [[1, 2]],
                [[3, 4], [6], [5]]
            ]
        ],
        [[[[5]]], [[[6], [5]]]]
    ];

    Proto3Schema ser = check new (Byte5DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Byte5DArray);
    Byte5DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testDecimal3DArray() returns error? {
    Decimal3DArray data = [
        [[1.90909, 2]],
        [[3, 4]],
        [[5e-19]],
        [[6]]
    ];

    Proto3Schema ser = check new (Decimal3DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (Decimal3DArray);
    Decimal3DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function testNestedArrayWithUnionFields() returns error? {
    TestMember member1 = {fullName: "foo bar", id: 100};
    UnionType[] uType = [1, 2, ["John", "Doe"], member1];
    UnionType[] uType2 = [4, 5];
    Level3Array[] level3Array = [uType, uType2];
    Level2Array[] level2Array = [level3Array];

    Proto3Schema ser = check new (Level1Array);
    byte[] encoded = check ser.serialize(level2Array);

    Proto3Schema des = check new (Level1Array);
    Level1Array decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, level2Array);
}

@test:Config {}
public isolated function testArrayofMaps() returns error? {
    AgeMapArray data = [{"Tony Hoare": 88},{"Linus Torvalds": 52}];

    Proto3Schema ser = check new (AgeMapArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (AgeMapArray);
    AgeMapArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}

@test:Config {}
public isolated function test2DArrayofMaps() returns error? {
    AgeMap2DArray data = [[{"Tony Hoare": 88}],[{"Linus Torvalds": 52}]];

    Proto3Schema ser = check new (AgeMap2DArray);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new (AgeMap2DArray);
    AgeMap2DArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}
