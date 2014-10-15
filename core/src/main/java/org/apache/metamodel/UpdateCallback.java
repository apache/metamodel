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

import org.apache.metamodel.create.TableCreatable;
import org.apache.metamodel.delete.RowDeletable;
import org.apache.metamodel.drop.TableDroppable;
import org.apache.metamodel.insert.RowInsertable;
import org.apache.metamodel.update.RowUpdateable;

/**
 * An {@link UpdateCallback} is used by an {@link UpdateScript} to perform
 * updates on a {@link DataContext}. Multiple updates (eg. insertion of several
 * rows or creation of multiple tables) can (and should) be performed with a
 * single {@link UpdateCallback}. This pattern guarantees that connections
 * and/or file handles are handled correctly, surrounding the
 * {@link UpdateScript} that is being executed.
 */
public interface UpdateCallback extends TableCreatable, TableDroppable, RowInsertable, RowUpdateable, RowDeletable {

    /**
     * Gets the DataContext on which the update script is being executed.
     * 
     * @return the DataContext on which the update script is being executed.
     */
    public DataContext getDataContext();
}
