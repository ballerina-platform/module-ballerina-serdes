// Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/serdes;
import ballerina/io;
import ballerina/http;
import ballerina/log;

isolated int errorCount = 0;
isolated int serializationCount = 0;
isolated int deserializationCount = 0;

type User record {
    readonly int id;
    string name;
    int age;
};

table<User> key(id) users = table [];

service /serdes on new http:Listener(9100) {

    function init() returns error? {
        check loadUserTable();
    }

    resource function post next(http:Request request) returns byte[]|error {
        serdes:Proto3Schema serdes = check new (User);
        byte[] encodedPayload = check request.getBinaryPayload();
        User|error user = handleDeserialization(serdes, encodedPayload);
        User nextUser = user is User ? users.get((user.id + 1) % (users.length() - 1)) : users.get(0);
        return handleSerialization(serdes, nextUser);
    }

    resource function get result() returns map<int> {
        map<int> result = {};
        lock {
            result["errorCount"] = errorCount;
        }
        lock {
            result["operationCount"] = serializationCount;
        }
        lock {
            result["operationCount"] = result.get("operationCount") + deserializationCount;
        }
        resetCounters();
        return result;
    }
}

function loadUserTable() returns error? {
    log:printInfo("Loading user table ...");
    json content = check io:fileReadJson("resources/users.json");
    User[] userArray = check content.cloneWithType();
    foreach User user in userArray {
        users.add(user);
    }
    log:printInfo("User table loaded.");
}

function resetCounters() {
    lock {
        errorCount = 0;
    }
    lock {
        serializationCount = 0;
    }
    lock {
        deserializationCount = 0;
    }
}

function handleSerialization(serdes:Schema serdes, User user) returns byte[] {
    byte[]|serdes:Error encoded = serdes.serialize(user);
    byte[] response = [];
    if encoded is serdes:Error {
        log:printError("Serialization faild: ", encoded);
        lock {
            errorCount += 1;
        }
    } else {
        lock {
            serializationCount += 1;
        }
        response = encoded;
    }
    return response;
}

function handleDeserialization(serdes:Schema serdes, byte[] payload) returns User|error {
    User|serdes:Error user = serdes.deserialize(payload);
    if user is serdes:Error {
        log:printError("Deserialization faild: ", user);
        lock {
            errorCount += 1;
        }
    } else {
        lock {
            deserializationCount += 1;
        }
    }
    return user;
}
