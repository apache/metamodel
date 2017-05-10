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
package org.apache.metamodel.query;

import java.io.Serializable;

/**
 * Defines the types of operators that can be used in filters.
 *
 * @see FilterItem
 */
public interface OperatorType extends Serializable {

    public static final OperatorType EQUALS_TO = new OperatorTypeImpl("=", false);

    public static final OperatorType DIFFERENT_FROM = new OperatorTypeImpl("<>", false);

    public static final OperatorType LIKE = new OperatorTypeImpl("LIKE", true);

    public static final OperatorType NOT_LIKE = new OperatorTypeImpl("NOT LIKE", true);

    public static final OperatorType GREATER_THAN = new OperatorTypeImpl(">", false);

    public static final OperatorType GREATER_THAN_OR_EQUAL = new OperatorTypeImpl(">=", false);

    public static final OperatorType LESS_THAN = new OperatorTypeImpl("<", false);

    public static final OperatorType LESS_THAN_OR_EQUAL = new OperatorTypeImpl("<=", false);

    public static final OperatorType IN = new OperatorTypeImpl("IN", true);

    public static final OperatorType NOT_IN = new OperatorTypeImpl("NOT IN", true);

    public static final OperatorType[] BUILT_IN_OPERATORS = new OperatorType[] { EQUALS_TO, DIFFERENT_FROM, LIKE,
            NOT_LIKE, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, IN, NOT_IN };

/**
     * Determines if this operator requires a space delimitor. Operators that are written using letters usually require
     * space delimitation whereas sign-based operators such as "=" and "&lt;" can be applied even without any delimitaton.
     * 
     * @return
     */
    public boolean isSpaceDelimited();

    public String toSql();

}
