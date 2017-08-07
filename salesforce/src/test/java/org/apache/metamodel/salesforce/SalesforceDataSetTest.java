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
package org.apache.metamodel.salesforce;

import com.sforce.soap.partner.QueryResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.TypeMapper;
import com.sforce.ws.parser.PullParserException;
import com.sforce.ws.parser.XmlInputStream;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SalesforceDataSetTest {

    @Test
    public void testQuery() throws Exception {
        QueryResult qr = queryResult("/result/double-value.xml");
        MutableColumn version = new MutableColumn("Version", ColumnType.DOUBLE);

        SalesforceDataSet dataSet = new SalesforceDataSet(Collections.singletonList(version), qr, null);
        List<Row> rows = dataSet.toRows();

        assertEquals(5386.21, rows.get(0).getValue(version));
        assertEquals(53.0, rows.get(1).getValue(version));
        
        dataSet.close();
    }

    private QueryResult queryResult(String input) throws PullParserException, IOException, ConnectionException {
        QueryResult queryResult = new QueryResult();
        XmlInputStream in = new XmlInputStream();
        in.setInput(getClass().getResourceAsStream(input), "UTF-8");
        queryResult.load(in, new TypeMapper());
        return queryResult;
    }
}