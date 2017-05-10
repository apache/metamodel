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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

final class DynamoDbUtils {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbUtils.class);

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

    public static ColumnType toColumnType(String attributeName, String attributeType) {
        if (attributeType == null) {
            return null;
        }
        switch (attributeType) {
        case "S":
            return ColumnType.STRING;
        case "N":
            return ColumnType.NUMBER;
        case "B":
            return ColumnType.BINARY;
        }
        logger.warn("Unexpected attribute type '{}' for attribute: {}", attributeType, attributeName);
        return null;
    }

    public static Object toValue(AttributeValue a) {
        if (a == null || Boolean.TRUE == a.isNULL()) {
            return null;
        }
        // dynamo is a bit funky this way ... it has a getter for each possible
        // data type.
        return firstNonNull(a.getB(), a.getBOOL(), a.getBS(), a.getL(), a.getM(), a.getN(), a.getNS(), a.getS(), a
                .getSS());
    }

    private static Object firstNonNull(Object... objects) {
        for (Object object : objects) {
            if (object != null) {
                return object;
            }
        }
        return null;
    }
}
