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
package org.apache.metamodel.neo4j.utils;

import org.apache.metamodel.schema.ColumnType;
import org.json.JSONObject;

public interface ColumnTypeHandler {
    /**
     * Sets the following node in the chain. 
     * @param successor
     */
    void setSuccessor(final ColumnTypeHandler successor);

    /**
     * Returns a column type based on the given value.  
     * @param data
     * @param key
     * @return ColumnType
     */
    ColumnType getTypeFromValue(final JSONObject data, final String key);
}
