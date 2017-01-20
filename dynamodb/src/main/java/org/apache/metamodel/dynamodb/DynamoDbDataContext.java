package org.apache.metamodel.dynamodb;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoDbDataContext extends QueryPostprocessDataContext implements Closeable {

    private static final String SCHEMA_NAME = "public";
    
    private final AmazonDynamoDB _client;
    private final boolean _shutdownOnClose;

    public DynamoDbDataContext() {
        this(AmazonDynamoDBClientBuilder.defaultClient(), true);
    }

    public DynamoDbDataContext(AmazonDynamoDB client) {
        this(client, false);
    }

    private DynamoDbDataContext(AmazonDynamoDB client, boolean shutdownOnClose) {
        _client = client;
        _shutdownOnClose = shutdownOnClose;
    }

    @Override
    public void close() {
        if (_shutdownOnClose) {
            _client.shutdown();
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(getMainSchemaName());
        final ListTablesResult tables = _client.listTables();
        final List<String> tableNames = tables.getTableNames();
        for (String tableName : tableNames) {
            final MutableTable table = new MutableTable(tableName, schema);
            schema.addTable(table);

            final DescribeTableResult tableDescription = _client.describeTable(tableName);
            final List<AttributeDefinition> attributeDefinitions = tableDescription.getTable()
                    .getAttributeDefinitions();
            for (AttributeDefinition attributeDefinition : attributeDefinitions) {
                final String attributeName = attributeDefinition.getAttributeName();
                final String attributeType = attributeDefinition.getAttributeType();
                final MutableColumn column = new MutableColumn(attributeName, table);
                table.addColumn(column);
                column.setNativeType(attributeType);
            }
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        List<String> attributeNames = new ArrayList<>(columns.length);
        for (String attributeName : attributeNames) {
            attributeNames.add(attributeName);
        }
        ScanRequest scanRequest = new ScanRequest(table.getName());
        scanRequest.setAttributesToGet(attributeNames);
        if (maxRows > 0) {
            scanRequest.setLimit(maxRows);
        }
        final ScanResult result = _client.scan(scanRequest);
        return new DynamoDbDataSet(columns, result);
    }
}
