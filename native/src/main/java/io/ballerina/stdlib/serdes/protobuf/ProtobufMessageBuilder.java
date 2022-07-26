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

import com.google.protobuf.DescriptorProtos.DescriptorProto.Builder;

import java.util.Comparator;
import java.util.HashMap;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;

/**
 * Dynamically creates a Protocol Buffer message type.
 */
public class ProtobufMessageBuilder {

    private final Builder messageDescriptorProtoBuilder;
    private final HashMap<String, ProtobufMessageBuilder> nestedMessages = new HashMap<>();
    private final HashMap<String, ProtobufMessageFieldBuilder> messageFields = new HashMap<>();
    private final String messageName;
    private final ProtobufMessageBuilder parentMessage;

    public ProtobufMessageBuilder(String msgName) {
        this(msgName, null);
    }

    public ProtobufMessageBuilder(String msgName, ProtobufMessageBuilder parentMessage) {
        messageName = msgName;
        messageDescriptorProtoBuilder = DescriptorProto.newBuilder();
        messageDescriptorProtoBuilder.setName(msgName);
        this.parentMessage = parentMessage;
    }

    public String getName() {
        return messageName;
    }

    public Builder getProtobufMessage() {
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
        Builder nestedProtobufMessage = nestedMessage.getProtobufMessage();
        boolean isDefined = nestedMessages.get(nestedMessage.getName()) != null;
        if (!isDefined) {
            messageDescriptorProtoBuilder.addNestedType(nestedProtobufMessage);
            nestedMessages.put(nestedMessage.getName(), nestedMessage);
        }
    }

    public boolean hasMessageDefinitionInMessageTree(String targetMsgName) {
        if (messageName.equals(targetMsgName)) {
            return true;
        }
        boolean hasMessage = nestedMessages.get(targetMsgName) != null;
        return hasMessage || (parentMessage != null && parentMessage.hasMessageDefinitionInMessageTree(targetMsgName));
    }

    @Override
    public String toString() {
        return toString("");
    }

    private String toString(String space) {
        String protoStart = space + "message " + messageName + " {\n";
        String levelSpace = space + "  ";
        StringBuilder msgContent = new StringBuilder();

        // Build string for nested types
        nestedMessages.values()
                .forEach(nestedMessage -> msgContent.append(nestedMessage.toString(levelSpace)).append("\n"));

        // Build string for field
        messageFields.values().stream().sorted(Comparator.comparingInt(ProtobufMessageFieldBuilder::getFieldNumber))
                .forEach(messageField -> msgContent.append(messageField.toString(levelSpace)));

        String protoEnd = space + "}\n";

        return protoStart + msgContent + protoEnd;
    }
}
