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
package org.apache.metamodel.solr;

import static org.junit.Assert.*;

import java.util.List;
import java.util.ArrayList;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class SolrDataContextTest {
    private static String _url   = null;
    private static String _index = null;

    private static DataContext _dataContext;

    private static boolean _configured;

    private static String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        Properties properties = new Properties();
        File file             = new File(getPropertyFilePath());

        if (file.exists()) {
            properties.load(new FileReader(file));
            _url    = properties.getProperty("solr.url");
            _index  = properties.getProperty("solr.index");

            _configured = (_url != null && _index != null && !_url.isEmpty() && !_index.isEmpty());
        } else {
            _configured = false;
        }

        if (_configured) {
            _dataContext = new SolrDataContext(_url, _index);
        } else {
            return;
        }
    }

    @AfterClass
    public static void afterTests() {
        if (_configured) {
            System.out.println("Solr server shut down!");
        }
    }

    @Test
    public void testWhereWithLimit() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        DataSet dataSet = _dataContext
                .executeQuery("SELECT * FROM collection1 WHERE (manu LIKE '%Maxtor%' or manu LIKE '%Samsung%') LIMIT 1");
        try {
            assertTrue(dataSet.next());

            Row row = dataSet.getRow();
            Object val = row.getValue(1);

            assertEquals("1491367446686203904", val.toString());
        } finally {
            dataSet.close();
        }
    }

    @Test
    public void testGroupByQueryWithAlphaOrder() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        List<String> output = new ArrayList<String>();

        DataSet dataSet = _dataContext
                .executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu LIKE '%Maxtor%' OR manu LIKE '%Samsung%') GROUP BY manu ORDER BY manu LIMIT 3");

        while (dataSet.next()) {
            Row row = dataSet.getRow();
            output.add(row.toString());
        }

        assertEquals(
                "[Row[values=[0, a]], Row[values=[0, america]], Row[values=[0, apache]]]",
                output.toString());
    }

    @Test
    public void testGroupByQueryWithMeasureOrder() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        List<String> output = new ArrayList<String>();

        DataSet dataSet = _dataContext
                .executeQuery("SELECT COUNT(*) AS X, manu FROM collection1 WHERE (manu LIKE '%Maxtor%' OR manu LIKE '%Samsung%') GROUP BY manu ORDER BY X DESC LIMIT 3");

        while (dataSet.next()) {
            Row row = dataSet.getRow();
            output.add(row.toString());
        }

        assertEquals(
                "[Row[values=[1, corp]], Row[values=[1, maxtor]], Row[values=[0, a]]]",
                output.toString());
    }

    @Test
    public void testCountQuery() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        DataSet dataSet = _dataContext
                .executeQuery("SELECT COUNT(*) FROM collection1");

        try {
            assertTrue(dataSet.next());
            assertEquals("Row[values=[32]]", dataSet.getRow().toString());
        } finally {
            dataSet.close();
        }
    }

    @Test
    public void testCountWithWhereQuery() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        DataSet dataSet = _dataContext
                .executeQuery("SELECT COUNT(*) FROM collection1 WHERE (manu LIKE '%Samsung%' or manu LIKE '%Maxtor%')");

        try {
            assertTrue(dataSet.next());
            assertEquals("Row[values=[3]]", dataSet.getRow().toString());
        } finally {
            dataSet.close();
        }
    }

    @Test
    public void testQueryForANonExistingIndex() throws Exception {
        if (!_configured) {
            System.err.println(notConfiguredMessage());
            return;
        }

        boolean isThrown = false;

        try {
            DataSet dataSet = _dataContext
                    .executeQuery("SELECT COUNT(*) FROM foo");
        } catch (Exception e) {
            isThrown = true;
        } finally {

        }

        assertTrue(isThrown);
    }

    private String notConfiguredMessage() {
        return "Solr server not configured";
    }
}
