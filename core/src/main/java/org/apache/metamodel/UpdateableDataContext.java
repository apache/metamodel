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
package org.apache.metamodel;

/**
 * Represents a {@link DataContext} that supports updating write-operations.
 */
public interface UpdateableDataContext extends DataContext {

    /**
     * Submits an {@link UpdateScript} for execution on the {@link DataContext}.
     * 
     * Since implementations of the {@link DataContext} vary quite a lot, there
     * is no golden rule as to how an update script will be executed. But the
     * implementors should strive towards handling an {@link UpdateScript} as a
     * single transactional change to the data store.
     * 
     * @param update
     *            the update script to execute
     * @return a summary of the updates performed
     */
    public UpdateSummary executeUpdate(UpdateScript update);

}
