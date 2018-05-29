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
package org.apache.metamodel.hbase;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.schema.Table;

/**
 * A builder-class to delete rows based on their keys in a HBase datastore
 */
public class HBaseRowDeletionBuilder extends AbstractRowDeletionBuilder {

    private HBaseClient _hBaseClient;
    private Object _key;

    /**
     * Creates a {@link HBaseRowDeletionBuilder}
     * @param hBaseWriter
     * @param table
     * @throws IllegalArgumentException when the hBaseWriter is null
     */
    public HBaseRowDeletionBuilder(final HBaseClient hBaseWriter, final Table table) {
        super(table);
        if (hBaseWriter == null) {
            throw new IllegalArgumentException("hBaseClient cannot be null");
        }
        this._hBaseClient = hBaseWriter;
    }

    /** 
     * @throws MetaModelException when value is null
     */
    @Override
    public synchronized void execute() {
        if (_key == null) {
            throw new MetaModelException("Key cannot be null");
        }
        _hBaseClient.deleteRow(getTable().getName(), _key);
    }

    public void setKey(Object _key) {
        this._key = _key;
    }
}
