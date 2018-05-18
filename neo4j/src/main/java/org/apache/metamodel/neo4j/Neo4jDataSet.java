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
package org.apache.metamodel.neo4j;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.SelectItem;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;

final class Neo4jDataSet extends AbstractDataSet {

    private JSONObject _resultJSONObject;
    private int _currentRowIndex;
    private Row _row;

    public Neo4jDataSet(List<SelectItem> selectItems, JSONObject resultJSONObject) {
        super(selectItems);
        _resultJSONObject = resultJSONObject;
        _currentRowIndex = 0;
    }

    @Override
    public boolean next() {
        try {
            final JSONArray resultsArray = _resultJSONObject.getJSONArray("results");
            
            if (resultsArray.length() > 0) {
                final JSONObject results = resultsArray.getJSONObject(0);
                final JSONArray data = results.getJSONArray("data");
                
                if (_currentRowIndex < data.length()) {
                    final JSONObject row = data.getJSONObject(_currentRowIndex);
                    final JSONArray jsonValues = row.getJSONArray("row");
                    final Object[] objectValues = new Object[jsonValues.length()];
                    
                    for (int i = 0; i < jsonValues.length(); i++) {
                        objectValues[i] = jsonValues.get(i);
                    }
                    
                    _row = new DefaultRow(new SimpleDataSetHeader(getSelectItems()), objectValues);
                    _currentRowIndex++;
                    
                    return true;
                }
            } else {
                final JSONArray errorArray = _resultJSONObject.getJSONArray("errors");
                final JSONObject error = errorArray.getJSONObject(0);
                throw new IllegalStateException(error.toString());
            }
        } catch (final JSONException e) {
            throw new IllegalStateException(e);
        }
        return false;
    }

    @Override
    public Row getRow() {
        return _row;
    }

}
