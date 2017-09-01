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
package org.apache.metamodel.factory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.metamodel.cassandra.CassandraDataContextFactory;
import org.apache.metamodel.couchdb.CouchDbDataContextFactory;
import org.apache.metamodel.csv.CsvDataContextFactory;
import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContextFactory;
import org.apache.metamodel.elasticsearch.rest.ElasticSearchRestDataContextFactory;
import org.apache.metamodel.excel.ExcelDataContextFactory;
import org.apache.metamodel.fixedwidth.FixedWidthDataContextFactory;
import org.apache.metamodel.hbase.HbaseDataContextFactory;
import org.apache.metamodel.jdbc.JdbcDataContextFactory;
import org.apache.metamodel.json.JsonDataContextFactory;
import org.apache.metamodel.pojo.PojoDataContextFactory;
import org.apache.metamodel.salesforce.SalesforceDataContextFactory;
import org.apache.metamodel.xml.XmlDomDataContextFactory;
import org.apache.metamodel.xml.XmlSaxDataContextFactory;
import org.junit.Assert;
import org.junit.Test;

public class DataContextFactoryRegistryTest {

    @Test
    public void testLoadAllFactories() {
        final DataContextFactoryRegistry registry = DataContextFactoryRegistryImpl.getDefaultInstance();
        final Collection<DataContextFactory> factories = registry.getFactories();

        final List<Class<?>> factoryClasses = factories.stream().map(f -> f.getClass()).collect(Collectors.toList());

        Assert.assertTrue(factoryClasses.contains(CassandraDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(CsvDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(CouchDbDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(ElasticSearchDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(ElasticSearchRestDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(ExcelDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(FixedWidthDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(HbaseDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(JdbcDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(JsonDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(PojoDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(SalesforceDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(XmlDomDataContextFactory.class));
        Assert.assertTrue(factoryClasses.contains(XmlSaxDataContextFactory.class));
    }
}
