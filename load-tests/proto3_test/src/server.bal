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
import ballerina/time;

configurable int PORT = 9100;
const time:Seconds EXECUTION_TIME = 10;

time:Utc startedTime = time:utcNow();
time:Utc endedTime = time:utcNow();

isolated int errorCount = 0;
isolated int serializationCount = 0;
isolated int deserializationCount = 0;

boolean completed = false;

type User record {
    readonly int id;
    string name;
    int age;
};

table<User> key(id) users = table [];

service /serdes on new http:Listener(PORT) {

    function init() returns error? {
        check loadUserTable();
    }

    resource function get 'start() returns boolean {
        http:Client|error clientEP = new (string `http://localhost:${PORT}/serdes`);
        if clientEP is error {
            return false;
        }
        resetCountersAndTime();
        _ = start beginCommunication(clientEP);
        return true;
    }

    resource function post next(http:Request request) returns byte[]|error {
        serdes:Proto3Schema serdes = check new (User);
        byte[] encodedPayload = check request.getBinaryPayload();
        User nextUser = handleDeserialization(serdes, encodedPayload);
        return handleSerialization(serdes, nextUser);
    }

    resource function get result() returns boolean|map<string> {
        if completed {
            string errCount = "";
            string sentCount = "";
            string receivedCount = "";
            string time = time:utcDiffSeconds(endedTime, startedTime).toString();

            lock {
                errCount = errorCount.toBalString();
            }
            lock {
                sentCount = serializationCount.toString();
            }
            lock {
                receivedCount = deserializationCount.toString();
            }

            return {errorCount: errCount, time, sentCount, receivedCount};
        }
        return false;
    }
}

function loadUserTable() returns error? {
    io:println("Loading user table ...");
    json content = check io:fileReadJson("resources/users.json");
    User[] userArray = check content.cloneWithType();
    foreach User user in userArray {
        users.add(user);
    }
    io:println("User table loaded.");
}

function beginCommunication(http:Client clientEP) returns error? {
    completed = false;
    final int totalUsers = users.length();
    User user = users.get(0);

    time:Utc expiryTime = time:utcAddSeconds(startedTime, EXECUTION_TIME);
    while time:utcDiffSeconds(expiryTime, time:utcNow()) > 0d {
        serdes:Proto3Schema serdes = check new (User);
        byte[] encodedPayload = handleSerialization(serdes, user);
        byte[] encodedResponse = check clientEP->post("/next", encodedPayload);
        User response = handleDeserialization(serdes, encodedResponse);
        user = users.get((response.id + 1) % (totalUsers - 1));
    }

    completed = true;
    endedTime = time:utcNow();
    io:println("Started time: ", startedTime);
    io:println("Ended time: ", endedTime);
    io:println("Total time elasped: ", time:utcDiffSeconds(endedTime, startedTime), " seconds");
}

function resetCountersAndTime() {
    lock {
        errorCount = 0;
    }
    lock {
        serializationCount = 0;
    }
    lock {
        deserializationCount = 0;
    }

    startedTime = time:utcNow();
    endedTime = time:utcNow();
}

function handleSerialization(serdes:Schema serdes, User user) returns byte[] {
    byte[]|serdes:Error encoded = serdes.serialize(user);
    byte[] response = [];
    if encoded is serdes:Error {
        io:print("Serialization faild: ", user);
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

function handleDeserialization(serdes:Schema serdes, byte[] payload) returns User {
    User|serdes:Error user = serdes.deserialize(payload);
    User nextUser = users.get(0);
    if user is serdes:Error {
        io:print("Deserialization faild: ", user);
        lock {
            errorCount += 1;
        }
    } else {
        lock {
            deserializationCount += 1;
        }
        nextUser = users.get((user.id + 1) % (users.length() - 1));
    }
    return nextUser;
}
