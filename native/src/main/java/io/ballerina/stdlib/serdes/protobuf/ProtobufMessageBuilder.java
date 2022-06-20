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

import java.util.Comparator;
import java.util.HashMap;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;

/**
 * Dynamically creates a Protocol Buffer message type.
 */
public class ProtobufMessageBuilder {

    private final DescriptorProto.Builder messageDescriptorProtoBuilder;
    private final HashMap<String, ProtobufMessageBuilder> nestedMessages = new HashMap<>();
    private final HashMap<String, ProtobufMessageFieldBuilder> messageFields = new HashMap<>();
    private final String messageName;

    public ProtobufMessageBuilder(String msgName) {
        messageName = msgName;
        messageDescriptorProtoBuilder = DescriptorProto.newBuilder();
        messageDescriptorProtoBuilder.setName(msgName);
    }

    public String getName() {
        return messageName;
    }

    public DescriptorProto.Builder getProtobufMessage() {
        return messageDescriptorProtoBuilder;
    }

    public void addField(ProtobufMessageFieldBuilder messageFieldBuilder) {
        boolean isDefined = messageFields.get(messageFieldBuilder.getFieldName()) != null;
        if (!isDefined) {
            messageDescriptorProtoBuilder.addField(messageFieldBuilder.getMessageField());
            messageFields.put(messageFieldBuilder.getFieldName(), messageFieldBuilder);
        }
    }

    public void addNestedMessage(ProtobufMessageBuilder nestedMessage) {
        DescriptorProto.Builder nestedProtobufMessage = nestedMessage.getProtobufMessage();
        boolean isDefined = nestedMessages.get(nestedMessage.getName()) != null;
        if (!isDefined) {
            messageDescriptorProtoBuilder.addNestedType(nestedProtobufMessage);
            nestedMessages.put(nestedMessage.getName(), nestedMessage);
        }
    }

    @Override
    public String toString() {
        return toString("");
    }

    public String toString(String space) {
        String protoStart = space + "message " + messageName + " {\n";
        String levelSpace = space + "  ";
        StringBuilder msgContent = new StringBuilder();

        // Build string for nested types
        nestedMessages.values().forEach(
                nestedMessage -> msgContent.append(nestedMessage.toString(levelSpace)).append("\n"));

        // Build string for field
        messageFields.values().stream().sorted(
                Comparator.comparingInt(ProtobufMessageFieldBuilder::getFieldNumber)).forEach(
                messageField -> msgContent.append(messageField.toString(levelSpace)));

        String protoEnd = space + "}\n";

        return protoStart + msgContent + protoEnd;
    }
}
