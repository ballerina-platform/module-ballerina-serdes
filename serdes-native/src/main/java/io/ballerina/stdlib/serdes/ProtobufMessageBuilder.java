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

package io.ballerina.stdlib.serdes;

import com.google.protobuf.DescriptorProtos;

/**
* Message Builder Class.
*
*/
public class ProtobufMessageBuilder {
    // Describes a message type
    private DescriptorProtos.DescriptorProto.Builder messageBuilder;
    // private int oneofFieldIndex = 0;

    // Constructor with message name as parameter
    ProtobufMessageBuilder(String messageName) {
        messageBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        messageBuilder.setName(messageName);
    }

    // Add message field without default value
    public ProtobufMessageBuilder addField(String label, String type, String name, int number) {
        DescriptorProtos.FieldDescriptorProto.Label fieldLabel = ProtobufMessageField.getFieldLabel(label);
        addField(fieldLabel, type, name, number, null, null);
        return this;
    }
//  Add message field with default value - not used
/*
    public ProtobufMessageBuilder addField(String label, String type, String name, int number, String defaultValue) {
        DescriptorProtos.FieldDescriptorProto.Label fieldLabel = ProtobufMessageField.getFieldLabel(label);
        addField(fieldLabel, type, name, number, defaultValue, null);
        return this;
    }
*/

    public ProtobufMessageBuilder addNestedMessage(ProtobufMessage nestedMessage) {
        messageBuilder.addNestedType(nestedMessage.getProtobufMessage());
        return this;
    }

//  Add oneOfField - not used
/*
    public OneofFieldBuilder addOneofField(String oneofFieldName) {
        messageBuilder.addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName(oneofFieldName).build());
        return new OneofFieldBuilder(this, oneofFieldIndex++);
    }
*/

    public ProtobufMessage build() {

        return new ProtobufMessage(messageBuilder.build());
    }

    // Add a single field to a message
    public void addField(DescriptorProtos.FieldDescriptorProto.Label label, String type, String name, int number,
                            String defaultValue, OneofFieldBuilder oneofFieldBuilder) {
        // Describes a field within a message.
        DescriptorProtos.FieldDescriptorProto.Builder messageFieldBuilder = DescriptorProtos.FieldDescriptorProto
                                                                                            .newBuilder();

        messageFieldBuilder.setLabel(label);
        messageFieldBuilder.setName(name);
        messageFieldBuilder.setNumber(number);

        DescriptorProtos.FieldDescriptorProto.Type fieldType = ProtobufMessageField.getFieldType(type);
        if (fieldType != null) {
            // Primitive types
            messageFieldBuilder.setType(fieldType);
        } else {
            // Non primitive
            messageFieldBuilder.setTypeName(type);
        }

        // Not used
/*
        if (defaultValue != null) {
            messageFieldBuilder.setDefaultValue(defaultValue);
        }

        if (oneofFieldBuilder != null) {
            messageFieldBuilder.setOneofIndex(oneofFieldBuilder.getMessageIndex());
        }
*/

        messageBuilder.addField(messageFieldBuilder.build());
    }
}
