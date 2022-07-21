/*
 * Copyright (c) 2022, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.google.protobuf.DynamicMessage.Builder;
import io.ballerina.runtime.api.types.TableType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTable;

import java.util.List;
import java.util.stream.Collectors;

import static io.ballerina.stdlib.serdes.Constants.TABLE_ENTRY;

/**
 * {@link TableMessageSerializer} class handles serialization of ballerina tables.
 */
public class TableMessageSerializer extends MessageSerializer {


    public TableMessageSerializer(Builder dynamicMessageBuilder, Object anydata,
                                  BallerinaStructuredTypeMessageSerializer messageSerializer) {
        super(dynamicMessageBuilder, anydata, messageSerializer);
    }

    @Override
    public void setIntFieldValue(Object ballerinaInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setByteFieldValue(Integer ballerinaByte) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFloatFieldValue(Object ballerinaFloat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDecimalFieldValue(BDecimal ballerinaDecimal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStringFieldValue(BString ballerinaString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBooleanFieldValue(Boolean ballerinaBoolean) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableFieldValue(BTable<?, ?> ballerinaTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArrayFieldValue(BArray ballerinaArray) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnionFieldValue(Object unionValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTupleFieldValue(BArray ballerinaTuple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<MessageFieldData> getListOfMessageFieldData() {
        BTable<?, ?> table = (BTable<?, ?>) getAnydata();
        Type constrainedType = ((TableType) TypeUtils.getType(table)).getConstrainedType();
        Type referredConstrainedType = TypeUtils.getReferredType(constrainedType);
        return table.values().stream().map(value -> new MessageFieldData(TABLE_ENTRY, value, referredConstrainedType))
                .collect(Collectors.toList());
    }
}
