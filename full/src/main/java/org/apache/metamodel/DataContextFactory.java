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

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.sql.DataSource;

import org.apache.metamodel.cassandra.CassandraDataContext;
import org.apache.metamodel.couchdb.CouchDbDataContext;
import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.apache.metamodel.elasticsearch.rest.ElasticSearchRestDataContext;
import org.apache.metamodel.excel.ExcelConfiguration;
import org.apache.metamodel.excel.ExcelDataContext;
import org.apache.metamodel.fixedwidth.FixedWidthConfiguration;
import org.apache.metamodel.fixedwidth.FixedWidthDataContext;
import org.apache.metamodel.hbase.HBaseConfiguration;
import org.apache.metamodel.hbase.HBaseDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.json.JsonDataContext;
import org.apache.metamodel.mongodb.mongo3.MongoDbDataContext;
import org.apache.metamodel.openoffice.OpenOfficeDataContext;
import org.apache.metamodel.pojo.PojoDataContext;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.salesforce.SalesforceDataContext;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.sugarcrm.SugarCrmDataContext;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.SimpleTableDef;
import org.apache.metamodel.xml.XmlDomDataContext;
import org.ektorp.http.StdHttpClient.Builder;
import org.elasticsearch.client.Client;
import org.xml.sax.InputSource;

import com.datastax.driver.core.Cluster;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import io.searchbox.client.JestClient;

/**
 * A factory for DataContext objects. This class substantially easens the task
 * of creating and initializing DataContext objects and/or their strategies for
 * reading datastores.
 * 
 * @see DataContext
 */
public class DataContextFactory {

    public static final char DEFAULT_CSV_SEPARATOR_CHAR = CsvConfiguration.DEFAULT_SEPARATOR_CHAR;
    public static final char DEFAULT_CSV_QUOTE_CHAR = CsvConfiguration.DEFAULT_QUOTE_CHAR;

    private DataContextFactory() {
        // Prevent instantiation
    }

    /**
     * Creates a composite DataContext based on a set of delegate DataContexts.
     * 
     * Composite DataContexts enables cross-DataContext querying and unified
     * schema exploration
     * 
     * @param delegates
     *            an array/var-args of delegate DataContexts
     * @return a DataContext that matches the request
     */
    public static DataContext createCompositeDataContext(DataContext... delegates) {
        return new CompositeDataContext(delegates);
    }

    /**
     * Creates a composite DataContext based on a set of delegate DataContexts.
     * 
     * Composite DataContexts enables cross-DataContext querying and unified
     * schema exploration
     * 
     * @param delegates
     *            a collection of delegate DataContexts
     * @return a DataContext that matches the request
     */
    public static DataContext createCompositeDataContext(Collection<DataContext> delegates) {
        return new CompositeDataContext(delegates);
    }

    /**
     * Creates a DataContext that connects to a Salesforce.com instance.
     * 
     * @param username
     *            the Salesforce username
     * @param password
     *            the Salesforce password
     * @param securityToken
     *            the Salesforce security token
     * @return a DataContext object that matches the request
     */
    public static DataContext createSalesforceDataContext(String username, String password, String securityToken) {
        return new SalesforceDataContext(username, password, securityToken);
    }

    /**
     * Create a DataContext that connects to a SugarCRM system.
     * 
     * @param baseUrl
     *            the base URL of the system, e.g. http://localhost/sugarcrm
     * @param username
     *            the SugarCRM username
     * @param password
     *            the SugarCRM password
     * @param applicationName
     *            the name of the application you are connecting with
     * @return a DataContext object that matches the request
     */
    public static DataContext createSugarCrmDataContext(String baseUrl, String username, String password,
            String applicationName) {
        return new SugarCrmDataContext(baseUrl, username, password, applicationName);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file) {
        return createCsvDataContext(file, DEFAULT_CSV_SEPARATOR_CHAR, DEFAULT_CSV_QUOTE_CHAR);
    }

    /**
     * Creates a DataContext based on a JSON file
     */
    public static DataContext createJsonDataContext(File file) {
        return new JsonDataContext(file);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, char separatorChar, char quoteChar) {
        return createCsvDataContext(file, separatorChar, quoteChar, FileHelper.DEFAULT_ENCODING);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @param encoding
     *            the character encoding of the file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, char separatorChar, char quoteChar,
            String encoding) {
        final CsvConfiguration configuration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, encoding,
                separatorChar, quoteChar, CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        final CsvDataContext dc = new CsvDataContext(file, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param configuration
     *            the CSV configuration to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, CsvConfiguration configuration) {
        final CsvDataContext dc = new CsvDataContext(file, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, char separatorChar, char quoteChar) {
        return createCsvDataContext(inputStream, separatorChar, quoteChar, FileHelper.DEFAULT_ENCODING);
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, char separatorChar, char quoteChar,
            String encoding) {
        final CsvConfiguration configuration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, encoding,
                separatorChar, quoteChar, CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        final CsvDataContext dc = new CsvDataContext(inputStream, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param configuration
     *            the CSV configuration to use
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, CsvConfiguration configuration) {
        final CsvDataContext dc = new CsvDataContext(inputStream, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param fileEncoding
     *            the character encoding of the file.
     * @param fixedValueWidth
     *            the (fixed) width of values in the file.
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, String fileEncoding, int fixedValueWidth) {
        return createFixedWidthDataContext(file, new FixedWidthConfiguration(
                FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, fileEncoding, fixedValueWidth));
    }

    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param configuration
     *            the fixed width configuration to use
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, FixedWidthConfiguration configuration) {
        final FixedWidthDataContext dc = new FixedWidthDataContext(file, configuration);
        return dc;
    }
    /**
    * Creates a DataContext based on a fixed width file.
    * 
    * @param resource
    *            the resource to read from.
    * @param configuration
    *            the fixed width configuration to use
    * @return a DataContext object that matches the request
    */
   public static DataContext createFixedWidthDataContext(Resource resource, FixedWidthConfiguration configuration) {
       final FixedWidthDataContext dc = new FixedWidthDataContext(resource, configuration);
       return dc;
   }


    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param fileEncoding
     *            the character encoding of the file.
     * @param fixedValueWidth
     *            the (fixed) width of values in the file.
     * @param headerLineNumber
     *            the line number of the column headers.
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, String fileEncoding, int fixedValueWidth,
            int headerLineNumber) {
        return createFixedWidthDataContext(file, new FixedWidthConfiguration(
                FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, fileEncoding, fixedValueWidth));
    }

    /**
     * Creates a DataContet based on an Excel spreadsheet file
     * 
     * @param file
     *            an excel spreadsheet file
     * @param configuration
     *            the configuration with metadata for reading the spreadsheet
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createExcelDataContext(File file, ExcelConfiguration configuration) {
        return new ExcelDataContext(file, configuration);
    }

    /**
     * Creates a DataContext based on an Excel spreadsheet file
     * 
     * @param file
     *            an Excel spreadsheet file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createExcelDataContext(File file) {
        return createExcelDataContext(file, new ExcelConfiguration());
    }

    /**
     * Creates a DataContext based on XML-content from an input source.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param inputSource
     *            an input source feeding XML content
     * @param schemaName
     *            the name to be used for the main schema
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(InputSource inputSource, String schemaName,
            boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(inputSource, schemaName, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on XML-content from a File.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param file
     *            the File to use for feeding XML content
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(File file, boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(file, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on XML-content from a URL.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param url
     *            the URL to use for feeding XML content
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(URL url, boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(url, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on an OpenOffice.org database file.
     * 
     * @param file
     *            an OpenOffice.org database file
     * @return a DataContext object that matches the request
     */
    public static DataContext createOpenOfficeDataContext(File file) {
        return new OpenOfficeDataContext(file);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection) {
        return new JdbcDataContext(connection);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds) {
        return new JdbcDataContext(ds);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param catalogName
     *            a catalog name to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, String catalogName) {
        return new JdbcDataContext(connection, TableType.DEFAULT_TABLE_TYPES, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, TableType... tableTypes) {
        return new JdbcDataContext(connection, tableTypes, null);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param catalogName
     *            a catalog name to use
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, String catalogName,
            TableType[] tableTypes) {
        return new JdbcDataContext(connection, tableTypes, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, TableType... tableTypes) {
        return new JdbcDataContext(ds, tableTypes, null);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param catalogName
     *            a catalog name to use
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, String catalogName,
            TableType[] tableTypes) {
        return new JdbcDataContext(ds, tableTypes, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param catalogName
     *            a catalog name to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, String catalogName) {
        return new JdbcDataContext(ds, TableType.DEFAULT_TABLE_TYPES, catalogName);
    }

    /**
     * Creates a new MongoDB datacontext.
     * 
     * @param hostname
     *            The hostname of the MongoDB instance
     * @param port
     *            the port of the MongoDB instance, or null if the default port
     *            should be used.
     * @param databaseName
     *            the name of the database
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @param tableDefs
     *            an array of table definitions, or null if table definitions
     *            should be autodetected.
     * @return a DataContext object that matches the request
     */
    @SuppressWarnings("resource")
    public static UpdateableDataContext createMongoDbDataContext(String hostname, Integer port, String databaseName,
            String username, char[] password, SimpleTableDef[] tableDefs) {
        try {
            final ServerAddress serverAddress;
            if (port == null) {
                serverAddress = new ServerAddress(hostname);
            } else {
                serverAddress = new ServerAddress(hostname, port);
            }
            final MongoClient mongoClient;
            final MongoDatabase mongoDb;
            if (Strings.isNullOrEmpty(username)) {
                mongoClient = new MongoClient(serverAddress);
            } else {
                final MongoCredential credential = MongoCredential.createCredential(username, databaseName, password);
                mongoClient = new MongoClient(serverAddress, Arrays.asList(credential));
            }
            mongoDb = mongoClient.getDatabase(databaseName);

            if (tableDefs == null || tableDefs.length == 0) {
                return new MongoDbDataContext(mongoDb);
            }
            return new MongoDbDataContext(mongoDb, tableDefs);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates a new MongoDB datacontext.
     * 
     * @param hostname
     *            The hostname of the MongoDB instance
     * @param port
     *            the port of the MongoDB instance, or null if the default port
     *            should be used.
     * @param databaseName
     *            the name of the database
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createMongoDbDataContext(String hostname, Integer port, String databaseName,
            String username, char[] password) {
        return createMongoDbDataContext(hostname, port, databaseName, username, password, null);
    }

    /**
     * Creates a new CouchDB datacontext.
     * 
     * @param hostname
     *            The hostname of the CouchDB instance
     * @param port
     *            the port of the CouchDB instance, or null if the default port
     *            should be used.
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCouchDbDataContext(String hostname, Integer port, String username,
            String password) {
        return createCouchDbDataContext(hostname, port, username, password, null);
    }

    /**
     * Creates a new CouchDB datacontext.
     * 
     * @param hostname
     *            The hostname of the CouchDB instance
     * @param port
     *            the port of the CouchDB instance, or null if the default port
     *            should be used.
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @param tableDefs
     *            an array of table definitions, or null if table definitions
     *            should be autodetected.
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCouchDbDataContext(String hostname, Integer port, String username,
            String password, SimpleTableDef[] tableDefs) {

        final Builder httpClientBuilder = new Builder();
        httpClientBuilder.host(hostname);
        if (port != null) {
            httpClientBuilder.port(port);
        }
        if (username != null) {
            httpClientBuilder.username(username);
        }
        if (password != null) {
            httpClientBuilder.password(password);
        }

        // increased timeouts (20 sec) - metamodel typically does quite some
        // batching so it might take a bit of time to provide a connection.
        httpClientBuilder.connectionTimeout(20000);
        httpClientBuilder.socketTimeout(20000);

        if (tableDefs == null || tableDefs.length == 0) {
            return new CouchDbDataContext(httpClientBuilder);
        }
        return new CouchDbDataContext(httpClientBuilder, tableDefs);
    }

    /**
     * Creates a new JSON-based ElasticSearch datacontext.
     * @param client
     *       The Jest client
     * @param indexName
     *       The ElasticSearch index name
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createElasticSearchDataContext(JestClient client, String indexName) {
        return new ElasticSearchRestDataContext(client, indexName);
    }

    /**
     * Creates a new ElasticSearch datacontext.
     * 
     * @param client
     *            The ElasticSearch client
     * @param indexName
     *            The ElasticSearch index name
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createElasticSearchDataContext(Client client, String indexName) {
        return new ElasticSearchDataContext(client, indexName);
    }

    /**
     * Creates a new Cassandra datacontext.
     * 
     * @param cluster
     *            The Cassandra client
     * @param keySpaceName
     *            The Cassandra key space name
     * @return a DataContext object that matches the request
     */
    public static DataContext createCassandraDataContext(Cluster cluster, String keySpaceName) {
        return new CassandraDataContext(cluster, keySpaceName);
    }
    
	/**
	 * Creates a new HBase datacontext.
	 * 
	 * @param configuration
	 *            {@code HBaseConfiguration} object containing detailed HBase
	 *            configuration properties.
	 * 
	 * @return a DataContext object that matches the request
	 */
    public static DataContext createHBaseDataContext(HBaseConfiguration configuration){
    	return new HBaseDataContext(configuration);
    }
    
	/**
	 * Creates a new HBase datacontext.
	 * 
	 * @param configuration
	 *            {@code HBaseConfiguration} object containing detailed HBase
	 *            configuration properties.
	 * 
	 * @param connection
	 *            A cluster connection encapsulating lower level individual
	 *            connections to actual servers and a connection to zookeeper.
	 * 
	 * @return a DataContext object that matches the request
	 */
	public static DataContext createHBaseDataContext(HBaseConfiguration configuration,org.apache.hadoop.hbase.client.Connection connection) {
		return new HBaseDataContext(configuration, connection);
	}
	
	/**
	 * Creates a new POJO data context that is empty but can be populated at
	 * will.
	 * 
	 * @return a DataContext object that matches the request
	 * 
	 */
	public static DataContext createPojoDataContext() {
		return new PojoDataContext();
	}

	/**
	 * Creates a new POJO data context based on the provided
	 * {@link TableDataProvider}s.
	 * 
	 * @param tables
	 *            list of tables
	 * 
	 * @return DataContext object that matches the request
	 */
	public static DataContext createPojoDataContext(List<TableDataProvider<?>> tables) {
		return new PojoDataContext(tables);
	}

	/**
	 * Creates a new POJO data context based on the provided
	 * {@link TableDataProvider}s.
	 * 
	 * @param schemaName
	 *            the name of the created schema
	 * 
	 * @param tables
	 *            table information
	 * 
	 * @return DataContext object that matches the request
	 * 
	 */
	public static DataContext createPojoDataContext(String schemaName,TableDataProvider<?>[] tables) {
		return new PojoDataContext(schemaName, tables);
	}

	/**
	 * Creates a new POJO data context based on the provided
	 * {@link TableDataProvider}s.
	 * 
	 * @param schemaName
	 *            the name of the created schema
	 * 
	 * @param tables
	 *            list of tables
	 * 
	 * @return DataContext object that matches the request
	 */
	public static DataContext createPojoDataContext(String schemaName,List<TableDataProvider<?>> tables) {
		return new PojoDataContext(schemaName, tables);
	}
}