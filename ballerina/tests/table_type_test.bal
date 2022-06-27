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

type Row record {
    int id;
    string name;
};

type RecordTable table<Row>;

type Score map<int>;
type ScoreTable table<Score>;

type Item record {
    readonly int id;
    string name;
    decimal price;
};

type ItemTable table<Item> key(id);
type ArrayOfTable RecordTable[];

type RecordWithTableField record {
    ScoreTable scoreTable;
    RecordTable recordTable;
};

type MapWithTableConstraint map<ScoreTable>;

@test:Config {}
public isolated function testTableWithRecord() returns error? {
    RecordTable data = table [
        {id: 1, name: "Plato"},
        {id: 2, name: "Aristotle"},
        {id:3, name: "Socrates"}
    ];

    Proto3Schema ser = check new(RecordTable);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(RecordTable);
    RecordTable decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}   

@test:Config {}
public isolated function testTableWithMap() returns error? {
    ScoreTable data = table [
        {"Manchester City" : 93},
        {"Liverpool": 92},
        {"Chelsea": 74}
    ];
    Proto3Schema ser = check new(ScoreTable);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(ScoreTable);
    ScoreTable decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}   


@test:Config {}
public isolated function testTableWithKey() returns error? {
    ItemTable data = table [
            {id: 1, name: "Item A", price: 1e10},
            {id: 20, name: "Item X", price: 2e5}
        ];
    Proto3Schema ser = check new(ItemTable);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(ItemTable);
    ItemTable decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
    test:assertEquals(decoded[20], data[20]);
}   

@test:Config {}
public isolated function testArrayOfTables() returns error? {
    ArrayOfTable data = [
        table [
                {id: 1, name: "Plato"},
                {id: 2, name: "Aristotle"}
            ],
        table [{id: 3, name: "Socrates"}]
    ];

    Proto3Schema ser = check new(ArrayOfTable);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(ArrayOfTable);
    ArrayOfTable decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}   

@test:Config {}
public isolated function testTableRecordField() returns error? {
    RecordWithTableField data = {
        recordTable: table [
                {id: 1, name: "Plato"},
                {id: 2, name: "Aristotle"},
                {id: 3, name: "Socrates"}
            ],
        scoreTable: table [
                {"Manchester City": 93},
                {"Liverpool": 92},
                {"Chelsea": 74}
            ]
    };

    Proto3Schema ser = check new(RecordWithTableField);
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(RecordWithTableField);
    RecordWithTableField decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}  


@test:Config {}
public isolated function testTableAsMapConstraint() returns error? {
    MapWithTableConstraint data = {
        "Football clubs": table [
                {"Manchester City": 93},
                {"Liverpool": 92},
                {"Chelsea": 74}
            ],
        "NBA clubs": table [
            {"Boston celtics": 90, 
             "Golden state warriors": 103
             }
        ]
    };

    Proto3Schema ser = check new(MapWithTableConstraint);
    check ser.generateProtoFile("MapWithTableConstraint.proto");
    byte[] encoded = check ser.serialize(data);

    Proto3Schema des = check new(MapWithTableConstraint);
    MapWithTableConstraint decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, data);
}  
