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

import ballerina/io;
import ballerina/http;
import ballerina/time;
import ballerina/serdes;

type User record {
    readonly int id;
    string name;
    int age;
};

const time:Seconds EXECUTION_TIME = 3600;

public function main(string label, string outputCsvPath) returns error? {
    http:Client loadTestClient = check new ("http://bal.perf.test", httpVersion = http:HTTP_1_1);

    serdes:Proto3Schema serdes = check new (User);
    User user = {id: 0, name: "default", age: 0};
    byte[] encodedPayload = check serdes.serialize(user);
    byte[] defaultPayload = encodedPayload;

    time:Utc startedTime = time:utcNow();
    time:Utc expiryTime = time:utcAddSeconds(startedTime, EXECUTION_TIME);
    io:println("Communication started");

    time:Utc timer = time:utcNow();
    int sampleCount = 0;
    while time:utcDiffSeconds(expiryTime, time:utcNow()) > 0d {
        do {
            byte[] encodedResponse = check loadTestClient->post("/serdes/next", encodedPayload);
            if encodedResponse.length() > 0 {
                encodedPayload = encodedResponse;
            } else {
                encodedPayload = defaultPayload;
            }
            sampleCount += 1;
            if isOneMinutePassed(timer) {
                user = check serdes.deserialize(encodedPayload);
                io:println("User: ", user);
                timer = time:utcNow();
            }
        } on fail error e {
            io:println(e);
        }
    }

    time:Utc endedTime = time:utcNow();
    time:Seconds timeElasped = time:utcDiffSeconds(endedTime, startedTime);
    io:println("Communication ended: ", {
        "Started time": startedTime,
        "Ended time": endedTime,
        "Total time elasped": timeElasped
    });

    map<int> testResults = check loadTestClient->get("/serdes/result");
       
    int errorCount = testResults.get("errorCount");
    int operationCount = testResults.get("operationCount");
    any[] results = [
        label, sampleCount, <float>timeElasped / <float>operationCount,
        0, 0, 0, 0, 0, 0, <float>errorCount / <float>sampleCount,
        <float>operationCount / <float>timeElasped, 0, 0, time:utcNow()[0], 0, 1];
    check writeResultsToCsv(results, outputCsvPath);
}

function isOneMinutePassed(time:Utc timer) returns boolean {
    return time:utcDiffSeconds(time:utcNow(), timer) > 60d;
}

function writeResultsToCsv(any[] results, string outputPath) returns error? {
    string[] finalResult = [];
    foreach var result in results {
        finalResult.push(result.toString());
    }
    check io:fileWriteCsv(outputPath, [finalResult], io:APPEND);
}
