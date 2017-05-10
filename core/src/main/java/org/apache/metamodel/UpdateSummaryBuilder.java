/**
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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A builder object for {@link UpdateSummary}.
 */
public class UpdateSummaryBuilder {

    private int _inserts;
    private int _updates;
    private int _deletes;
    private Set<Object> _generatedKeys;

    public UpdateSummaryBuilder() {
    }

    public UpdateSummary build() {
        final Integer insertedRows = (_inserts == -1 ? null : _inserts);
        final Integer updatedRows = (_updates == -1 ? null : _updates);
        final Integer deletedRows = (_deletes == -1 ? null : _deletes);
        final Iterable<Object> generatedKeys;
        if (_generatedKeys != null) {
            generatedKeys = new LinkedHashSet<>(_generatedKeys);
        } else {
            generatedKeys = null;
        }
        return new DefaultUpdateSummary(insertedRows, updatedRows, deletedRows, generatedKeys);
    }

    public UpdateSummaryBuilder addInsert() {
        return addInserts(1);
    }

    public UpdateSummaryBuilder addInserts(int inserts) {
        if (_inserts != -1) {
            _inserts += inserts;
        }
        return this;
    }

    public UpdateSummaryBuilder makeInsertsUnknown() {
        _inserts = -1;
        return this;
    }

    public UpdateSummaryBuilder addUpdate() {
        return addUpdates(1);
    }

    public UpdateSummaryBuilder addUpdates(int updates) {
        if (_updates != -1) {
            _updates += updates;
        }
        return this;
    }

    public UpdateSummaryBuilder makeUpdatesUnknown() {
        _updates = -1;
        return this;
    }

    public UpdateSummaryBuilder addDelete() {
        return addDeletes(1);
    }

    public UpdateSummaryBuilder addDeletes(int deletes) {
        if (_deletes != -1) {
            _deletes += deletes;
        }
        return this;
    }

    public UpdateSummaryBuilder makeDeletesUnknown() {
        _deletes = -1;
        return this;
    }

    public UpdateSummaryBuilder addGeneratedKey(Object key) {
        if (_generatedKeys == null) {
            _generatedKeys = new HashSet<>();
        }
        _generatedKeys.add(key);
        return this;
    }

    public UpdateSummaryBuilder addGeneratedKeys(Object... keys) {
        if (_generatedKeys == null) {
            _generatedKeys = new HashSet<>();
        }
        for (Object key : keys) {
            _generatedKeys.add(key);
        }
        return this;
    }

    public UpdateSummaryBuilder addGeneratedKeys(Iterable<?> keys) {
        if (_generatedKeys == null) {
            _generatedKeys = new HashSet<>();
        }
        for (Object key : keys) {
            _generatedKeys.add(key);
        }
        return this;
    }

    public UpdateSummaryBuilder makeGeneratedKeysUnknown() {
        _generatedKeys = null;
        return this;
    }
}
