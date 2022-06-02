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

type Contact record {
    string mobile;
    string home;
};

type Street record {
    string street1;
    string street2;
};

type Address record {
    Street street;
    string country;
};

type Person record {
   string name;
   int age;
   byte[] img;
   float random;
   Contact contact;
};

type Student record {
   string name;
   int age;
   byte[] img;
   Contact[] contacts;
   Address address;
};

type Primitive record {
    string stringValue;
    int intValue;
    float floatValue;
    boolean boolValue;
};

type Arrays record {
    string[] stringArray;
    int[] intArray;
    float[] floatArray;
    boolean[] boolArray;
    byte[] byteArray;
};

type StringArray string[];
type IntArray int[];
type ByteArray byte[];
type FloatArray float[];
type DecimalArray float[];
type BoolArray boolean[];

type RecordArray Contact[];

@test:Config{}
public isolated function testPrimitiveFloat() returns error? {
    Proto3Schema ser = check new(float);
    byte[] encoded = check ser.serialize(6.666);

    Proto3Schema des = check new(float);
    float decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, 6.666);
}

@test:Config{}
public isolated function testPrimitiveDecimal() returns error? {
    Proto3Schema ser = check new(decimal);
    byte[] encoded = check ser.serialize(1.23);

    Proto3Schema des = check new(decimal);
    float decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, 1.23);
}

@test:Config{}
public isolated function testPrimitiveBoolean() returns error? {
    Proto3Schema ser = check new(boolean);
    byte[] encoded = check ser.serialize(true);

    Proto3Schema des = check new(boolean);
    boolean decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, true);
}

@test:Config{}
public isolated function testPrimitiveString() returns error? {
    Proto3Schema ser = check new(string);
    byte[] encoded = check ser.serialize("module-ballerina-serdes");

    Proto3Schema des = check new(string);
    string decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, "module-ballerina-serdes");
}

@test:Config{}
public isolated function testPrimitiveInt() returns error? {
    Proto3Schema ser = check new(int);
    byte[] encoded = check ser.serialize(666);

    Proto3Schema des = check new(int);
    int decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, 666);
}

@test:Config{}
public isolated function testStringArray() returns error? {
    Proto3Schema ser = check new(StringArray);
    byte[] encoded = check ser.serialize(["Jane", "Doe"]);

    Proto3Schema des = check new(StringArray);
    StringArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, ["Jane", "Doe"]);
}

@test:Config{}
public isolated function testIntArray() returns error? {
    Proto3Schema ser = check new(IntArray);
    byte[] encoded = check ser.serialize([1, 2, 3]);

    Proto3Schema des = check new(IntArray);
    IntArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, [1, 2, 3]);
}

@test:Config{}
public isolated function testByteArray() returns error? {
    Proto3Schema ser = check new(ByteArray);
    byte[] encoded = check ser.serialize(base16 `aeeecdefabcd12345567888822`);

    Proto3Schema des = check new(ByteArray);
    ByteArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, [174,238,205,239,171,205,18,52,85,103,136,136,34]);
}

@test:Config{}
public isolated function testFloatArray() returns error? {
    Proto3Schema ser = check new(FloatArray);
    byte[] encoded = check ser.serialize([0.123, 4.968, 3.256]);

    Proto3Schema des = check new(FloatArray);
    FloatArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, [0.123, 4.968, 3.256]);
}

@test:Config{}
public isolated function testDecimalArray() returns error? {
    Proto3Schema ser = check new(DecimalArray);
    byte[] encoded = check ser.serialize([0.123, 4.968, 3.256]);

    Proto3Schema des = check new(DecimalArray);
    FloatArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, [0.123, 4.968, 3.256]);
}

@test:Config{}
public isolated function testBooleanArray() returns error? {
    Proto3Schema ser = check new(BoolArray);
    byte[] encoded = check ser.serialize([true, false, true, false]);

    Proto3Schema des = check new(BoolArray);
    BoolArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, [true, false, true, false]);
}

type InnerArray int[];
type OuterArray InnerArray[];

@test:Config{}
public isolated function testNestedArray() returns error? {
    InnerArray i1 = [1, 2, 3];
    InnerArray i2 = [4, 5, 6];
    OuterArray I = [i1, i2];

    Proto3Schema ser = check new(OuterArray);
    byte[] encoded = check ser.serialize(I);

    Proto3Schema des = check new(OuterArray);
    OuterArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, I);
}

@test:Config{}
public isolated function testRecordWithPrimitives() returns error? {
    Primitive primitiveRecord = { stringValue: "serdes", intValue: 192, floatValue: 192.168, boolValue: false };

    Proto3Schema ser = check new(Primitive);
    byte[] encoded = check ser.serialize(primitiveRecord);

    Proto3Schema des = check new(Primitive);
    Primitive decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, primitiveRecord);
}

@test:Config{}
public isolated function testRecordWithArrays() returns error? {
    Arrays arrayRecord = {
        stringArray: ["Jane", "Doe"],
        intArray: [1, 2, 3],
        floatArray: [0.123, 4.968, 3.256],
        boolArray: [true, false, true, false],
        byteArray: base16 `aeeecdefabcd12345567888822`
    };

    Proto3Schema ser = check new(Arrays);
    byte[] encoded = check ser.serialize(arrayRecord);

    Proto3Schema des = check new(Arrays);
    Arrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, arrayRecord);
}

@test:Config{}
public isolated function testNestedRecord() returns error? {
    byte[] byteArray = base16 `aeeecdefabcd12345567888822`;
    Contact phone = {mobile: "+94111111111", home: "+94777777777"};
    Person president = { name: "Joe",  age:70, img:byteArray, random:1.666, contact:phone };

    Proto3Schema ser = check new(Person);
    byte[] encoded = check ser.serialize(president);

    Proto3Schema des = check new(Person);
    Person decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, president);
}

@test:Config{}
public isolated function testArrayOfRecords() returns error? {
    Contact phone1 = {mobile: "+123456", home: "789"};
    Contact phone2 = {mobile: "+456789", home: "123"};
    Contact[] contacts = [phone1, phone2];

    Proto3Schema ser = check new(RecordArray);
    byte[] encoded = check ser.serialize(contacts);

    Proto3Schema des = check new(RecordArray);
    RecordArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, contacts);
}

@test:Config{}
public isolated function testComplexRecord() returns error? {
    byte[] byteArray = base16 `aeeecdefabcd12345567888822`;
    Contact phone1 = {mobile: "+123456", home: "789"};
    Contact phone2 = {mobile: "+456789", home: "123"};
    Street street = { street1: "random lane", street2: "random street" };
    Address address = { street: street, country: "Sri Lanka" };
    Contact[] nums = [phone1, phone2];
    Student john = { name: "John Doe", age: 21, img: byteArray, contacts: nums, address: address };

    Proto3Schema ser = check new(Student);
    byte[] encoded = check ser.serialize(john);

    Proto3Schema des = check new(Student);
    Student decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, john);
}

type Member record {
    string name;
    decimal? salary;
    Contact contact?;
};

@test:Config{}
public isolated function testRecordWithNil() returns error? {
    Member member2 = {name: "bar", salary:()};

    Proto3Schema ser = check new(Member);
    byte[] encoded = check ser.serialize(member2);

    Proto3Schema des = check new(Member);
    Member decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, member2);
}

type DecimalOrNil decimal?;

@test:Config{}
public isolated function testFieldWithNil() returns error? {
    Proto3Schema ser = check new(DecimalOrNil);
    byte[] encoded = check ser.serialize(());

    Proto3Schema des = check new(DecimalOrNil);
    DecimalOrNil decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, ());
}

type Union int|string|boolean|();

@test:Config{}
public isolated function testUnionField() returns error? {
    Proto3Schema ser = check new(Union);
    byte[] encoded = check ser.serialize(128);

    Proto3Schema des = check new(Union);
    Union decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, 128);
}

type UnionWithArrays int[]|string[]|boolean;

@test:Config{}
public isolated function testUnionWithArrays() returns error? {
    int[] nums = [1, 9, 2, 1, 6, 8];

    Proto3Schema ser = check new(UnionWithArrays);
    byte[] encoded = check ser.serialize(nums);

    Proto3Schema des = check new(UnionWithArrays);
    UnionWithArrays decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nums);
}

type UnionMember record {
    string name;
    int id;
};

type UnionWithRecord int|string[]|UnionMember|();

@test:Config{}
public isolated function testUnionWithRecord() returns error? {
    UnionMember member = { name: "Jane", id:100 };

    Proto3Schema ser = check new(UnionWithRecord);
    byte[] encoded = check ser.serialize(member);

    Proto3Schema des = check new(UnionWithRecord);
    UnionWithRecord decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, member);
}

type UnionElement int|string[]|UnionMember|();
type UnionArray UnionElement[];

@test:Config{}
public isolated function testArrayOfUnionElements() returns error? {
    UnionMember member = { name: "Jane", id:100 };
    UnionElement[] array = [1, 2, ["John", "Doe"], member];

    Proto3Schema ser = check new(UnionArray);
    byte[] encoded = check ser.serialize(array);

    Proto3Schema des = check new(UnionArray);
    UnionArray decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, array);
}

type RecordWithUnionFields record {
    string name;
    UnionElement membership;
};

@test:Config{}
public isolated function testRecordWithUnionFields() returns error? {
    RecordWithUnionFields rec = { name: "Jane", membership: () };

    Proto3Schema ser = check new(RecordWithUnionFields);
    byte[] encoded = check ser.serialize(rec);

    Proto3Schema des = check new(RecordWithUnionFields);
    RecordWithUnionFields decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, rec);
}

type UnionType int|string[]|TestMember[]|TestMember;
type Level3Array UnionType[];
type Level2Array Level3Array[];
type Level1Array Level2Array[];

@test:Config{}
public isolated function testNestedArrayWithUnionFields() returns error? {
    TestMember member1 = {full_name: "foo bar", id: 100};
    UnionType[] uType = [1, 2, ["John", "Doe"], member1];
    UnionType[] uType2 = [4, 5];
    Level3Array[] level3Array = [uType, uType2];
    Level2Array[] level2Array = [level3Array];

    Proto3Schema ser = check new(Level1Array);
    byte[] encoded = check ser.serialize(level2Array);

    Proto3Schema des = check new(Level1Array);
    Level1Array decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, level2Array);
}

type IntStringOrNull int|string|();

type RecordWithUnion record {
    UnionType member_id;
};

type MyRecord record {
    string name;
    UnionType test_type;
    RecordWithUnion member;
};

type TestMember record {
    string full_name;
    int id;
};

@test:Config{}
public isolated function testComplexTypeWithUnion() returns error? {
    TestMember[] member1 = [{full_name: "foo bar", id: 100}];
    RecordWithUnion membership = {member_id: 619};
    MyRecord randomRecord = {name: "John", test_type: member1, member: membership};

    Proto3Schema ser = check new(MyRecord);
    byte[] encoded = check ser.serialize(randomRecord);

    Proto3Schema des = check new(MyRecord);
    MyRecord decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, randomRecord);
}

@test:Config{}
public isolated function testUnionTypeWithNull() returns error? {
    IntStringOrNull nullType = ();

    Proto3Schema ser = check new(IntStringOrNull);
    byte[] encoded = check ser.serialize(nullType);

    Proto3Schema des = check new(IntStringOrNull);
    IntStringOrNull decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, nullType);
}

type Employer record {
    string name;
    int id;
    byte[] img;
};

type Employee record {
    string name;
    int id;
    byte[] img;
    string department;
};

type Individual Employer|Employee;

@test:Config{}
public isolated function testUseCase1() returns error? {
    byte[] byteArray = base16 `aeeecdefabcd12345567888822`;
    Employer employer = {name: "John Doe", id: 101, img: byteArray};

    Proto3Schema ser = check new(Individual);
    byte[] encoded = check ser.serialize(employer);

    Proto3Schema des = check new(Individual);
    Individual decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, employer);
}

type Cont record {
    string mobile;
};

type President record {
    string name;
    Cont contact;
};

@test:Config{}
public isolated function simpleTest() returns error? {
    President p = {name: "John Doe", contact: {mobile: "123456"}};

    Proto3Schema ser = check new(President);
    byte[] encoded = check ser.serialize(p);

    Proto3Schema des = check new(President);
    President decoded = check des.deserialize(encoded);
    test:assertEquals(decoded, p);
}
