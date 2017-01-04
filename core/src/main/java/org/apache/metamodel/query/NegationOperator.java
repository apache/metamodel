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
 * Defines the negation of conditions in filters.
 *
 * @see FilterItem
 */
public interface NegationOperator extends Serializable {

    public static final NegationOperator NONE = new NegationOperatorImpl("", false);

    public static final NegationOperator NOT = new NegationOperatorImpl("NOT", true);

    public static final NegationOperator[] BUILT_IN_OPERATORS = new NegationOperator[] { NONE, NOT };

/**
     * Determines if this operator requires a space delimitor. Operators that are written using letters usually require
     * space delimitation whereas sign-based operators such as "=" and "&lt;" can be applied even without any delimitaton.
     * 
     * @return
     */
    public boolean isSpaceDelimited();

    public String toSql();

}
