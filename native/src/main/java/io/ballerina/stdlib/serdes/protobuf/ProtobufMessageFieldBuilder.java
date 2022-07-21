/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.serdes.protobuf;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static io.ballerina.stdlib.serdes.Constants.EMPTY_STRING;
import static io.ballerina.stdlib.serdes.Constants.REPEATED_LABEL;
import static io.ballerina.stdlib.serdes.Constants.SPACE;

/**
 * Creates a Protocol Buffer message field.
 */
public class ProtobufMessageFieldBuilder {

    private final FieldDescriptorProto.Builder messageFieldBuilder;
    private final String fieldLabel;
    private final String fieldName;
    private final String fieldType;
    private final int fieldNumber;

    public ProtobufMessageFieldBuilder(String label, String type, String name, int number) {
        FieldDescriptorProto.Label fieldLabel = ProtobufMessageFieldProperties.getFieldLabel(label);
        messageFieldBuilder = FieldDescriptorProto.newBuilder();

        this.fieldLabel = label;
        fieldNumber = number;
        fieldType = type;
        fieldName = name;

        messageFieldBuilder.setLabel(fieldLabel);
        messageFieldBuilder.setName(name);
        messageFieldBuilder.setNumber(number);

        FieldDescriptorProto.Type fieldType = ProtobufMessageFieldProperties.getFieldType(type);
        if (fieldType != null) {
            messageFieldBuilder.setType(fieldType);
        } else {
            messageFieldBuilder.setTypeName(type);
        }
    }

    public FieldDescriptorProto getMessageField() {
        return messageFieldBuilder.build();
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldLabel() {
        return fieldLabel;
    }

    public String getFieldType() {
        return fieldType;
    }

    public int getFieldNumber() {
        return fieldNumber;
    }

    public String toString(String indentation) {
        String fieldLabel = getFieldLabel().equals(REPEATED_LABEL) ? REPEATED_LABEL + SPACE : EMPTY_STRING;
        return indentation + SPACE + fieldLabel + getFieldType() + SPACE + getFieldName() + SPACE + " = "
                + getFieldNumber() + ";\n";
    }

}
