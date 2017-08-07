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
package org.apache.metamodel.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class DynamoDbDataContextIntegrationTest {

    private AmazonDynamoDB client;

    @Before
    public void before() throws Exception {
        final String userHome = System.getProperty("user.home");
        final String propertiesFilename = userHome + "/metamodel-integrationtest-configuration.properties";
        final File file = new File(propertiesFilename);

        Assume.assumeTrue(file.exists());

        final Properties props = new Properties();
        props.load(new FileReader(file));

        final String accessKey = props.getProperty("dynamodb.accessKey");
        final String secretKey = props.getProperty("dynamodb.secretKey");

        Assume.assumeNotNull(accessKey, secretKey);

        final Regions region = Regions.fromName(props.getProperty("dynamodb.region", Regions.DEFAULT_REGION.getName()));

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        client = AmazonDynamoDBClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider).build();
    }

    @After
    public void after() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testScenario1() throws Exception {
        final String tableName = "MetaModel-" + UUID.randomUUID().toString();

        final ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(5l, 5l);

        final Collection<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(new AttributeDefinition("id", ScalarAttributeType.S));
        attributes.add(new AttributeDefinition("counter", ScalarAttributeType.N));

        final Collection<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement("id", KeyType.HASH));

        // CreateDateIndex
        final GlobalSecondaryIndex globalSecondaryIndex = new GlobalSecondaryIndex().withIndexName("counter_index")
                .withProvisionedThroughput(provisionedThroughput).withKeySchema(new KeySchemaElement()
                        .withAttributeName("counter").withKeyType(KeyType.HASH)).withProjection(new Projection()
                                .withProjectionType(ProjectionType.INCLUDE).withNonKeyAttributes("foundation"));

        final CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(tableName);
        createTableRequest.setAttributeDefinitions(attributes);
        createTableRequest.setGlobalSecondaryIndexes(Arrays.asList(globalSecondaryIndex));
        createTableRequest.setKeySchema(keySchema);
        createTableRequest.setProvisionedThroughput(provisionedThroughput);
        final CreateTableResult createTableResult = client.createTable(createTableRequest);

        // await the table creation to be "done".
        {
            String tableStatus = createTableResult.getTableDescription().getTableStatus();
            while (!"ACTIVE".equals(tableStatus)) {
                System.out.println("Waiting for table status to be ACTIVE. Currently: " + tableStatus);
                Thread.sleep(300);
                tableStatus = client.describeTable(tableName).getTable().getTableStatus();
            }
        }

        client.putItem(tableName, createItem("id", "foo", "counter", 1, "foundation", "Apache"));
        client.putItem(tableName, createItem("id", "bar", "counter", 2, "project", "MetaModel"));
        client.putItem(tableName, createItem("id", "baz", "counter", 3, "url", "http://metamodel.apache.org"));

        final SimpleTableDef[] tableDefs = new SimpleTableDef[] { new SimpleTableDef(tableName, new String[] {
                "counter", "project" }, new ColumnType[] { ColumnType.INTEGER, ColumnType.STRING }) };

        try {
            try (final DynamoDbDataContext dc = new DynamoDbDataContext(client, tableDefs)) {

                final Table table = dc.getTableByQualifiedLabel(tableName);
                assertEquals(tableName, table.getName());

                // Right now we can only discover indexed columns
                assertEquals("[id, counter, project, foundation]", Arrays.toString(table.getColumnNames().toArray()));

                final Column idColumn = table.getColumnByName("id");
                assertEquals(true, idColumn.isPrimaryKey());
                assertEquals(true, idColumn.isIndexed());
                assertEquals(ColumnType.STRING, idColumn.getType());
                assertEquals("Primary index member ('HASH' type)", idColumn.getRemarks());

                final Column counterColumn = table.getColumnByName("counter");
                assertEquals(ColumnType.NUMBER, counterColumn.getType());
                assertEquals(true, counterColumn.isIndexed());
                assertEquals(false, counterColumn.isPrimaryKey());
                assertEquals("counter_index member ('HASH' type)", counterColumn.getRemarks());

                final Column projectColumn = table.getColumnByName("project");
                assertEquals(ColumnType.STRING, projectColumn.getType());
                assertEquals(false, projectColumn.isIndexed());
                assertEquals(false, projectColumn.isPrimaryKey());
                assertEquals(null, projectColumn.getRemarks());

                final Column foundationColumn = table.getColumnByName("foundation");
                assertEquals(null, foundationColumn.getType());
                assertEquals(false, foundationColumn.isIndexed());
                assertEquals(false, foundationColumn.isPrimaryKey());
                assertEquals("counter_index non-key attribute", foundationColumn.getRemarks());

                try (final DataSet dataSet = dc.query().from(table).select("id", "counter", "project").orderBy("id")
                        .execute()) {
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[bar, 2, MetaModel]]", dataSet.getRow().toString());
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[baz, 3, null]]", dataSet.getRow().toString());
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[foo, 1, null]]", dataSet.getRow().toString());
                    assertFalse(dataSet.next());
                }

                try (final DataSet dataSet = dc.query().from(tableName).select("counter", "project").where("id").eq(
                        "baz").execute()) {
                    assertTrue(dataSet instanceof InMemoryDataSet);

                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[3, null]]", dataSet.getRow().toString());
                    assertFalse(dataSet.next());
                }
            }
        } finally {
            client.deleteTable(tableName);
        }
    }

    // convenience method
    private Map<String, AttributeValue> createItem(Object... keyAndValues) {
        final Map<String, AttributeValue> map = new HashMap<>();
        for (int i = 0; i < keyAndValues.length; i = i + 2) {
            final String key = (String) keyAndValues[i];
            final Object value = keyAndValues[i + 1];
            map.put(key, DynamoDbUtils.toAttributeValue(value));
        }
        return map;
    }
}
