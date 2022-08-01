## Package overview

This package provides APIs for serializing and deserializing subtypes of Ballerina anydata type.

### Proto3Schema

An instance of the `serdes:Proto3Schema` class is used to serialize and deserialize ballerina values using protocol buffers.

#### Create a `serdes:Proto3Schema` object

```ballerina
// Define a type which is a subtype of anydata.
type Student record {
    int id;
    string name;
    decimal fees;
};

// Create a schema object by passing the type.
serdes:Proto3Schema schema = check new (Student);
```
While instantiation of this object, an underlying proto3 schema generated for the provided typedesc.

#### Serialization

```ballerina
Student student = {
    id: 7894,
    name: "Liam",
    fees: 24999.99
};

// Serialize the record value to bytes.
byte[] bytes = check schema.serialize(student);
```
A value having the same type as the provided type can be serialized by invoking the `serialize` method on the previously instantiated `serdes:Proto3Schema` object. The underlying implementation uses the previously generated proto3 schema to serialize the provided value.

#### Deserialization

```ballerina
type Student record {
    int id;
    string name;
    decimal fees;
};

byte[] bytes = readSerializedDataToByteArray();
serdes:Proto3Schema schema = check new (Student);

// Deserialize the record value from bytes.
Student student = check schema.deserialize(bytes);
```
The serialized value (`byte[]`) can be again deserialized by invoking the `deserialize` method on the instantiated `serdes:Proto3Schema` object. The underlying implementation uses the previously generated proto3 schema and deserializes the provided `byte[]`. As the result of deserialization the method returns the ballerina value with the type represented by the typedesc value provided during the `serdes:Proto3Schema` object instantiation.
