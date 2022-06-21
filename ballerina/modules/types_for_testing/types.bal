// RecordWithMultidimentionalArrays used to testDeserializationError
public type RecordWithMultidimentionalArrays record {
    decimal[] decimal2DArray; // this field is not cosistent with serilization side should be decimal[][]
    string[][][] string3DArray;
};
