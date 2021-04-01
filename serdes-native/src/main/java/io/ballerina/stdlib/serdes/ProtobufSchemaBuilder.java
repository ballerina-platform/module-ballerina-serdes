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
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

/**
* Schema Builder class.
*
*/
public class ProtobufSchemaBuilder {
    // Describes a complete .proto file
    private DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder;
    // FileDescriptorSet containing the .proto files the compiler parses.
    private DescriptorProtos.FileDescriptorSet.Builder fileDescriptorSetBuilder;

    // Constructor
    private ProtobufSchemaBuilder(String schema) {
        fileDescriptorProtoBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        fileDescriptorSetBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();
        fileDescriptorProtoBuilder.setName(schema);
    }

    public static ProtobufSchemaBuilder newSchemaBuilder(String schema) {
        return new ProtobufSchemaBuilder(schema);
    }

    // Add message to proto schema
    public void addMessageToProtoSchema(ProtobufMessage message) {
        DescriptorProtos.DescriptorProto protobufMessage = message.getProtobufMessage();
        fileDescriptorProtoBuilder.addMessageType(protobufMessage);
    }

    // Parses the .proto file and builds FileDescriptors
    public Descriptors.Descriptor build() throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet.Builder newFileDescriptorSetBuilder = DescriptorProtos.FileDescriptorSet
                                                                                                    .newBuilder();
        newFileDescriptorSetBuilder.addFile(fileDescriptorProtoBuilder.build());
        newFileDescriptorSetBuilder.mergeFrom(fileDescriptorSetBuilder.build());
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = newFileDescriptorSetBuilder.build();

        Descriptors.FileDescriptor fileDescriptor = null;
        for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : fileDescriptorSet.getFileList()) {
            List<Descriptors.FileDescriptor> resolvedFileDescriptors = new ArrayList<>();

            /* Resolve import dependencies
            List<String> dependencies = fileDescriptorProto.getDependencyList();
            Map<String, Descriptors.FileDescriptor> resolvedFileDescMap = new HashMap<String, Descriptors
                                                                                                .FileDescriptor>();

            for (String dependency : dependencies) {
                Descriptors.FileDescriptor fd = resolvedFileDescMap.get(dependency);
                if (fd != null) resolvedFileDescriptors.add(fd);
            }
            */

            Descriptors.FileDescriptor[] fileDescriptorArray = new Descriptors
                                                                    .FileDescriptor[resolvedFileDescriptors.size()];
            fileDescriptor = Descriptors.FileDescriptor
                                .buildFrom(fileDescriptorProto, resolvedFileDescriptors.toArray(fileDescriptorArray));
        }
        Descriptors.Descriptor messageBuilder = null;
        for (Descriptors.Descriptor messageType : fileDescriptor.getMessageTypes()) {
            messageBuilder = messageType;
        }

        return messageBuilder;
    }
}
