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
* Message Builder Class that creates a Protobuf Message with the provided fields.
*
*/
public class ProtobufMessageBuilder {

    // Describes a message type
    private DescriptorProtos.DescriptorProto.Builder messageBuilder;

    // Constructor with message name as parameter
    ProtobufMessageBuilder(String messageName) {
        messageBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        messageBuilder.setName(messageName);
    }

    // Add message field without default value
    public ProtobufMessageBuilder addField(String label, String type, String name, int number) {
        DescriptorProtos.FieldDescriptorProto.Label fieldLabel = ProtobufMessageField.getFieldLabel(label);
        addField(fieldLabel, type, name, number, null);
        return this;
    }

    // Add a message that is defined within a message
    public ProtobufMessageBuilder addNestedMessage(ProtobufMessage nestedMessage) {
        messageBuilder.addNestedType(nestedMessage.getProtobufMessage());
        return this;
    }

    public ProtobufMessage build() {
        return new ProtobufMessage(messageBuilder.build());
    }

    // Add a single field to a message
    public void addField(DescriptorProtos.FieldDescriptorProto.Label label, String type, String name, int number,
                            String defaultValue) {

        // Describes a field within a message.
        DescriptorProtos.FieldDescriptorProto.Builder messageFieldBuilder = DescriptorProtos.FieldDescriptorProto
                                                                                            .newBuilder();
        messageFieldBuilder.setLabel(label);
        messageFieldBuilder.setName(name);
        messageFieldBuilder.setNumber(number);
        DescriptorProtos.FieldDescriptorProto.Type fieldType = ProtobufMessageField.getFieldType(type);
        if (fieldType != null) {
            messageFieldBuilder.setType(fieldType);
        } else {
            messageFieldBuilder.setTypeName(type);
        }
        messageBuilder.addField(messageFieldBuilder.build());
    }
}
