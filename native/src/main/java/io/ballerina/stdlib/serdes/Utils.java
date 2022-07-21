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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;

import java.util.Locale;

import static io.ballerina.stdlib.serdes.Constants.CURLY_BRACE;

/**
 * Utility functions of SerDes module.
 *
 * @since 0.1.0
 */
public class Utils {

    public static final String SERDES_ERROR = "Error";
    private static Module serdesModule = null;

    public static Module getModule() {
        return serdesModule;
    }

    @SuppressWarnings("unused")
    public static void setModule(Environment env) {
        serdesModule = env.getCurrentModule();
    }

    public static BError createSerdesError(String message, String typeId) {
        return ErrorCreator.createError(getModule(), typeId, StringUtils.fromString(message), null, null);
    }

    public static String typeNotSupportedErrorMessage(Type type) {
        return "Type `" + type + "` not supported, use a reference type instead: " + "`type MyType " + type + ";`";
    }

    public static String typeNotSupportedErrorMessage(RecordType type) {
        String recordTypeWithoutModulePrefix = type.toString().split(":")[1];
        return "Type `" + recordTypeWithoutModulePrefix + "` not supported, use a reference type instead: "
                + "`type MyType " + recordTypeWithoutModulePrefix + ";`";
    }

    // Get the dimention of given array type
    public static int getArrayDimensions(ArrayType array) {
        int dimension = 1;
        Type basicElementType = TypeUtils.getReferredType(array.getElementType());
        while (basicElementType.getTag() == TypeTags.ARRAY_TAG) {
            array = (ArrayType) TypeUtils.getReferredType(array.getElementType());
            basicElementType = TypeUtils.getReferredType(array.getElementType());
            dimension++;
        }

        return dimension;
    }

    // Get the basic ballerina type of the given array
    public static Type getBaseElementTypeOfBallerinaArray(ArrayType array) {
        Type basicElementType = TypeUtils.getReferredType(array.getElementType());
        while (basicElementType.getTag() == TypeTags.ARRAY_TAG) {
            array = (ArrayType) TypeUtils.getReferredType(array.getElementType());
            basicElementType = TypeUtils.getReferredType(array.getElementType());
        }
        return basicElementType;
    }

    // Get the basic ballerina type name of the given array
    public static String getBaseElementTypeNameOfBallerinaArray(ArrayType array) {
        return getBaseElementTypeOfBallerinaArray(array).getName();
    }

    public static boolean isAnonymousBallerinaRecord(Type ballerinaType) {
        return ballerinaType.getName().contains(CURLY_BRACE);
    }

    // Create protobuf message name for the given ballerina primitive type (string -> StringValue)
    public static String createMessageName(String ballerinaPrimitiveType) {
        return ballerinaPrimitiveType.substring(0, 1).toUpperCase(Locale.ENGLISH) + ballerinaPrimitiveType.substring(
                1) + Constants.VALUE_SUFFIX;
    }
}
