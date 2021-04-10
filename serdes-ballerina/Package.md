## Package Overview

This package provides Ballerina anydata serialization/deserialization except for inline records, table type, and enum.

Serialization:
```
Proto3SerDes serializer = new(dataType);

byte[] encoded = serializer.serialize(value);
```

Deserialization:

```
Proto3SerDes deserializer = new(dataType);

dataType decoded = <dataType> deserializer.deserialize(encoded);
```
