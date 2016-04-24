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
package org.apache.metamodel.schema.naming;

import java.io.Closeable;

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

/**
 * Represents a 'session' in which a single {@link Table}'s {@link Column}s are
 * named.
 */
public interface ColumnNamingSession extends Closeable {

    /**
     * Provides the name to apply for a given column.
     * 
     * @param ctx
     *            the context of the column naming taking place. This contains
     *            column index, intrinsic name etc. if available.
     * @return the name to provide to the column.
     */
    public String getNextColumnName(ColumnNamingContext ctx);

    /**
     * Ends the column naming session.
     */
    @Override
    public void close();
}
