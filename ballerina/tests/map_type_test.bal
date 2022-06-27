import ballerina/test;

type MapInt map<int>;
type MapFloat map<float>;
type MapString map<string>;
type MapByte map<byte>;
type MapDecimal map<decimal>;
type MapBoolean map<boolean>;

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