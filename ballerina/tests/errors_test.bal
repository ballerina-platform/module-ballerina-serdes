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

type EmployeeTable table<map<anydata>>;

@test:Config{}
public isolated function testUnsupportedDataType() returns error? {
    string expected = "Unsupported data type: anydata";

    Proto3Schema|error ser = new(EmployeeTable);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expected);
}

@test:Config{}
public isolated function testTypeMismatch() returns error? {
    string expected = "Failed to Serialize data: Type mismatch";

    Proto3Schema ser = check new(float);
    byte[]|error encoded = ser.serialize(123);

    test:assertTrue(encoded is Error);
    Error err = <Error> encoded;
    test:assertEquals(err.message(), expected);
}

type Chairman record {
    string name;
    string id;
    string department;
};

type Engineer record {
    string name;
    int id;
};

@test:Config{}
public isolated function testRecordTypeMismatch() returns error? {
    string expected = "Failed to Serialize data: Type mismatch";

    Engineer SE = {name: "Jane Doe", id: 123};

    Proto3Schema ser = check new(Chairman);
    byte[]|error encoded = ser.serialize(SE);

    test:assertTrue(encoded is Error);
    Error err = <Error> encoded;
    test:assertEquals(err.message(), expected);
}

type MapA map<int>;
type MapB map<float>;
type UnionOfMaps MapA|MapB;

@test:Config {}
public function testMapUnionMemberNotYetSupporteError() returns error? {
    string expectedErrorMsg = "Serdes not yet support map type as union member";

    Proto3Schema|error ser = new(UnionOfMaps);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}

@test:Config {}
public isolated function testMapArrayUnionMemberNotYetSupporteError() returns error? {
    string expectedErrorMsg = "Serdes not yet support array of maps as union member";

    Proto3Schema|error ser = new(UnionWithArrayOfMaps);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}

type RecordWithNonReferencedMapField record {
    map<int> ages;
};

@test:Config {}
public isolated function testRecordWithNonReferencedMapFieldError() returns error? {
    string expectedErrorMsg = "Record field of map type only supported with reference map type";

    Proto3Schema|error ser = new(RecordWithNonReferencedMapField);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}

type RecordWithNonReferencedTableField record {
    table<map<int>> ages;
};

@test:Config {}
public isolated function testRecordWithNonReferencedTableFieldError() returns error? {
    string expectedErrorMsg = "Record field of table type only supported with reference table type";

    Proto3Schema|error ser = new(RecordWithNonReferencedTableField);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}

type TableA table<map<int>>;
type TableB map<float>;
type UnionOfTables TableA|TableA;

@test:Config {}
public function testTableUnionMemberNotYetSupporteError() returns error? {
    string expectedErrorMsg = "Serdes not yet support table type as union member";

    Proto3Schema|error ser = new(UnionOfTables);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}

type UnionWithArrayOfTables TableA[]|TableB[][];

@test:Config {}
public isolated function testTableArrayUnionMemberNotYetSupporteError() returns error? {
    string expectedErrorMsg = "Serdes not yet support array of tables as union member";

    Proto3Schema|error ser = new(UnionWithArrayOfTables);
    
    test:assertTrue(ser is Error);
    Error err = <Error> ser;
    test:assertEquals(err.message(), expectedErrorMsg);
}
