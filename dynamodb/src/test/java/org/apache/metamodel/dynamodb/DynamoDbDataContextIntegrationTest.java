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
import org.apache.metamodel.schema.Table;
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
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
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

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).withCredentials(
                credentialsProvider).build();
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

        final Collection<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(new AttributeDefinition("id", ScalarAttributeType.S));

        final Collection<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement("id", KeyType.HASH));

        final CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(tableName);
        createTableRequest.setAttributeDefinitions(attributes);
        createTableRequest.setKeySchema(keySchema);
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(5l, 5l));
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

        client.putItem(tableName, createItem("id", "foo", "foundation", "Apache"));
        client.putItem(tableName, createItem("id", "bar", "project", "MetaModel"));
        client.putItem(tableName, createItem("id", "baz", "url", "http://metamodel.apache.org"));

        try {
            try (final DynamoDbDataContext dc = new DynamoDbDataContext(client)) {

                final Table table = dc.getTableByQualifiedLabel(tableName);
                assertEquals(tableName, table.getName());

                // Right now we can only discover indexed columns
                assertEquals("[id]", Arrays.toString(table.getColumnNames()));

                try (final DataSet dataSet = dc.query().from(table).selectAll().orderBy("id").execute()) {
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[bar]]", dataSet.getRow().toString());
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[baz]]", dataSet.getRow().toString());
                    assertTrue(dataSet.next());
                    assertEquals("Row[values=[foo]]", dataSet.getRow().toString());
                    assertFalse(dataSet.next());
                }
            }
        } finally {
            client.deleteTable(tableName);
        }
    }

    private Map<String, AttributeValue> createItem(String... keyAndValues) {
        final Map<String, AttributeValue> map = new HashMap<>();
        for (int i = 0; i + 1 < keyAndValues.length; i++) {
            final String key = keyAndValues[i];
            final String value = keyAndValues[i + 1];
            final AttributeValue attributeValue = new AttributeValue();
            attributeValue.setS(value);
            map.put(key, attributeValue);
        }
        return map;
    }
}
