/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.dynamodb;

import org.apache.metamodel.schema.ColumnType;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

final class DynamoDbUtils {

    // prevent instantiation
    private DynamoDbUtils() {
    }

    public static AttributeValue toAttributeValue(Object value) {
        if (value instanceof AttributeValue) {
            return (AttributeValue) value;
        }
        final AttributeValue attributeValue = new AttributeValue();
        if (value == null) {
            attributeValue.setNULL(true);
        } else if (value instanceof Number) {
            attributeValue.setN(value.toString());
        } else if (value instanceof Boolean) {
            attributeValue.setBOOL((Boolean) value);
        } else if (value instanceof String) {
            attributeValue.setS(value.toString());
        } else {
            // TODO: Actually there's a few more value types we should support,
            // see AttributeValue.setXxxx
            throw new UnsupportedOperationException("Unsupported value type: " + value);
        }
        return attributeValue;
    }

    public static ScalarAttributeType toAttributeType(ColumnType type) {
        if (type == null) {
            return ScalarAttributeType.S;
        }
        if (type.isBinary()) {
            return ScalarAttributeType.B;
        }
        if (type.isNumber()) {
            return ScalarAttributeType.S;
        }
        // default to string
        return ScalarAttributeType.S;
    }
}
