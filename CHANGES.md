### Work-in-progress

 * [METAMODEL-136] - Added LIKE operator native support (using conversion to regex) for MongoDB.
 * [METAMODEL-138] - Allow empty characteres before AS keyword.

### Apache MetaModel 4.3.3

 * [METAMODEL-123] - Added compatibility with ElasticSearch version 1.4.x
 * [METAMODEL-93] - Added compatibility with Apache HBase version 1.0.0
 * [METAMODEL-124] - Invoked ElasticSearch cross-version incompatible methods via reflection
 * [METAMODEL-125] - Added support for comma-separated select items in Query.select(String) method argument.
 * [METAMODEL-128] - Fixed bug in DataSet ordering when aggregation functions are applied to non-JDBC modules.
 * [METAMODEL-131] - Added support for composite primary keys in JDBC CREATE TABLE statements.

### Apache MetaModel 4.3.2

 * [METAMODEL-78] - Fixed a bug that caused SELECT DISTINCT to sometimes return duplicates records on certain DataContext implementations.
 * [METAMODEL-106] - Improved handling of invalid or non-existing index names passed to ElasticSearchDataContext
 * [METAMODEL-79] - Added update execution support on ElasticSearch module. Increased capability of pushing down WHERE items to ElasticSearch searches.
 * [METAMODEL-115] - Improved query parsing to allow lower-case function names, operators etc.

### Apache MetaModel 4.3.1

 * [METAMODEL-100] - Fixed bug when having multiple columns of same name. Added column no. comparison when calling Column.equals(...).

### Apache MetaModel 4.3.0-incubating

 * [METAMODEL-77] - New module 'elasticsearch' for connecting and modeling ElasticSearch indexes through MetaModel.
 * [METAMODEL-18] - New module 'cassandra' for connecting and modelling Apache Cassandra databases through MetaModel.
 * [METAMODEL-83] - Added new operator types: GREATER_THAN_OR_EQUAL ('>=') and LESS_THAN_OR_EQUAL ('<=').
 * [METAMODEL-92] - For JSON, MongoDB and CouchDB: Made it possible to specify column names referring nested fields such as "name.first" or "addresses[0].city".
 * [METAMODEL-76] - Query parser improved to handle filters without spaces inbetween operator and operands.
 * [METAMODEL-95] - Fixed a critical bug in the Salesforce.com module which caused all number values to be interpreted as '1'.
 * [METAMODEL-74] - Fixed a bug related to skipping blank values when applying an aggregate function (SUM, AVG etc.)
 * [METAMODEL-85] - Fixed a bug that caused NULL values to be evaluated with equal-sign in JDBC update and delete statements instead of 'IS NULL'.
 
### Apache MetaModel 4.2.0-incubating

 * [METAMODEL-38] - New module 'json' for handling json files (containing JSON arrays of documents or line-delimited JSON documents)
 * [METAMODEL-54] - ColumnType converted from enum to interface to allow for further specialization in modules.
 * [METAMODEL-57] - Changed the column type VARCHAR into STRING for the modules: CSV, Fixedwidth, Excel and XML.
 * [METAMODEL-56] - Made separate column types for converted JDBC LOBs - "CLOB as String" and "BLOB as bytes".
 * [METAMODEL-46] - Improved row-lookup by primary key (ID) in CouchDB
 * [METAMODEL-58] - Fixed a bug related to using CreateTable class and primary keys not getting created.
 * [METAMODEL-3]  - Improved writing of Byte-Order-Mark (BOM) for various encoding spelling variants.
 * [METAMODEL-70] - Made the build compatible with both JDK versions 6 and 7.
 * [METAMODEL-59] - Fixed a bug related to handling of date/time literals in MS SQL Server queries.
 * [METAMODEL-60] - Fixed a bug related to DISTINCT and TOP keywords in MS SQL Server queries.
 * [METAMODEL-45] - Improved and standardized way of handling integration test connection information towards external databases.
 * [METAMODEL-62] - Fixed a bug related to fault-tolerant handling of malformed CSV lines when reading CSVs in single-line mode
 * [METAMODEL-68] - Made it possible to create a CSV table without a header line in the file, if the user configures it.
 * [METAMODEL-67] - Upgraded Jackson (JSON library) dependency from org.codehaus namespace to the newer com.fasterxml namespace.
 * [METAMODEL-69] - Fixed issue with deserialization of ColumnType into the new interface instead of the old enum.

### Apache MetaModel 4.1.0-incubating

 * [METAMODEL-13] - Added support for Apache HBase via the new module "MetaModel-hbase"
 * [METAMODEL-41] - Added a parser for SimpleTableDef objects (SimpleTableDefParser). It parses statements similar to CREATE TABLE statements, although without the "CREATE TABLE" prefix. For example: foo (bar INTEGER, baz VARCHAR)
 * [METAMODEL-11] - New module "MetaModel-spring" which adds a convenient FactoryBean to produce various types of DataContext objects based on externalizable parameters, for Spring framework users.
 * [METAMODEL-32] - Fixed thread-safety issue in Excel module when tables (sheets) metadata is updated.
 * [METAMODEL-47] - Fixed issue in Excel of loading schema if query is fired based on metadata from a previous DataContext instance.
 * [METAMODEL-35] - Improved query rewriting for DB2 when paged queries contain ORDER BY clause.
 * [METAMODEL-44] - Added an optional method for QueryPostprocessDataContext implementations to do a row-lookup by primary key value.
 * [METAMODEL-43] - Made CSV datastores skip empty lines in file instead of treating them of rows with null values.
 * [METAMODEL-39] - Added pooling of active/used Connections and PreparedStatements in JDBC compiled queries.
 * [METAMODEL-34] - Updated LICENSE file to not include bundled dependencies' licenses.
 * [METAMODEL-33] - Ensured that Apache Rat plugin for Maven is properly activated.
 * [METAMODEL-37] - Removed old site sources from project.

### Apache MetaModel 4.0.0-incubating

 * [METAMODEL-9] - SalesforceDataSet is throwing exception for insert sql of record having date/time.
 * [METAMODEL-4] - Use folder name as schema name for file based DataContexts
 * [METAMODEL-5] - Faster CsvDataContext implementation for single-line values
 * [METAMODEL-26] - Provide endpoint URL with SalesforceDataContext
 * Upgraded Apache POI dependency to v. 3.9
 * Improved fluent Query builder API by adding string parameter based methods for joining tables
 * Added a utility ObjectInputStream for deserializing legacy MetaModel objects
 * Performance improvement to CSV reading when values are only single line based
 * Setting up the project on apache infrastructure
 * [METAMODEL-10] - Exclude Jackcess dependency (Access module) from MetaModel
 * Renaming the package hierarchy from org.eobjects.metamodel to org.apache.metamodel
 * [METAMODEL-29] - Fixed issue in CreateTable builder class, causing it to only support a single column definition
 * [METAMODEL-30] - Fixed issue with count(*) queries on CSV resources that does not provide a byte stream length 
