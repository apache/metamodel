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
package org.apache.metamodel.xml;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableRelationship;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.ImmutableRef;
import org.apache.metamodel.util.NumberComparator;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.UrlResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/**
 * A DataContext strategy that reads XML content and maps it to a table-based
 * model similar to the rest of MetaModel. Tables are created by examining the
 * data in the XML file, NOT by reading XML Schemas (xsd/dtd's). This enables
 * compliancy with ALL xml formats but also raises a risk that two XML files
 * with the same format wont nescesarily yield the same table model if some
 * optional attributes or tags are omitted in one of the files.
 * 
 * The parsing method applied in this datacontext is DOM based, which means that
 * at upon parsing (only a single point in time), the whole file will be read
 * and it's tree structure kept in memory. Therefore this DataContext is NOT
 * appropriate for large XML files (10's, 100's or 1000's of megabytes).
 * 
 * @see XmlSaxDataContext
 */
public class XmlDomDataContext extends QueryPostprocessDataContext {

    private static final Logger logger = LoggerFactory.getLogger(XmlDomDataContext.class);

    public static final String NATIVE_TYPE_PRIMARY_KEY = "Auto-generated primary key";
    public static final String NATIVE_TYPE_FOREIGN_KEY = "Auto-generated foreign key";
    public static final String NATIVE_TYPE_ATTRIBUTE = "XML Attribute";
    public static final String NATIVE_TYPE_TEXT = "XML Text";

    private static final String TEXT_CONTENT_TEMP_SUFFIX = "_metamodel_text_content";

    private final Supplier<InputSource> _inputSourceRef;
    private final Map<String, List<Object[]>> _tableData = new HashMap<String, List<Object[]>>();;
    private final String _schemaName;

    private MutableSchema _schema;
    private boolean _autoFlattenTables;

    /**
     * Creates an XML DataContext strategy based on an already parsed Document.
     * 
     * @param schemaName
     * @param document
     * @param autoFlattenTables
     */
    public XmlDomDataContext(String schemaName, Document document, boolean autoFlattenTables) {
        _autoFlattenTables = autoFlattenTables;
        _schemaName = schemaName;
        _schema = new MutableSchema(_schemaName);
        _inputSourceRef = null;
        loadSchema(document);
    }

    /**
     * Creates an XML DataContext strategy based on a file.
     * 
     * @param resource
     *            the resource to parse
     * @param autoFlattenTables
     *            a parameter indicating whether or not tags with only text
     *            content or a single attribute should be flattened with it's
     *            parent table
     * 
     * @throws IllegalArgumentException
     *             if the file does not exist
     */
    public XmlDomDataContext(Resource resource, boolean autoFlattenTables) throws IllegalArgumentException {
        _inputSourceRef = createInputSourceRef(resource);
        _schemaName = resource.getName();
        _autoFlattenTables = autoFlattenTables;
    }

    public XmlDomDataContext(File file, boolean autoFlattenTables) {
        this(new FileResource(file), autoFlattenTables);
    }

    public XmlDomDataContext(InputSource inputSource, String schemaName, boolean autoFlattenTables) {
        _inputSourceRef = new ImmutableRef<InputSource>(inputSource);
        _schemaName = schemaName;
        _autoFlattenTables = autoFlattenTables;
    }

    public XmlDomDataContext(URL url, boolean autoFlattenTables) throws IllegalArgumentException {
        this(new UrlResource(url), autoFlattenTables);
    }

    private static Supplier<InputSource> createInputSourceRef(final Resource resource) {
        return () -> {
                final InputStream in = resource.read();
                return new InputSource(in);
        };
    }

    /**
     * Creates an XML DataContext strategy based on a file.
     * 
     * @param file
     *            the file to parse
     */
    public XmlDomDataContext(File file) {
        this(file, true);
    }

    public boolean isAutoFlattenTables() {
        return _autoFlattenTables;
    }

    public void setAutoFlattenTables(boolean autoFlattenTables) {
        _autoFlattenTables = autoFlattenTables;
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        loadSchema();
        List<Object[]> tableData = _tableData.get(table.getName());
        if (tableData == null) {
            throw new IllegalStateException("No such table name: '" + table.getName() + "'. Valid table names are: "
                    + _tableData.keySet());
        }

        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
        final DataSetHeader header = new CachingDataSetHeader(selectItems);

        final List<Row> resultData = new ArrayList<Row>();
        for (Object[] tableDataRow : tableData) {
            if (maxRows == 0) {
                break;
            }
            maxRows--;
            Object[] dataValues = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                int columnNumber = column.getColumnNumber();
                // Some rows may not contain values for all columns
                // (attributes)
                if (columnNumber < tableDataRow.length) {
                    dataValues[i] = tableDataRow[columnNumber];
                } else {
                    dataValues[i] = null;
                }
            }
            resultData.add(new DefaultRow(header, dataValues));
        }

        return new InMemoryDataSet(header, resultData);
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaName;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        loadSchema();
        return _schema;
    }

    /**
     * Forces a fresh load of the schema, even though it has already been loaded
     */
    public XmlDomDataContext reloadSchema() {
        _schema = null;
        loadSchema();
        return this;
    }

    /**
     * Loads the schema if it hasn't been loaded before
     */
    public XmlDomDataContext loadSchema() {
        if (_schema == null) {
            _schema = new MutableSchema(_schemaName);
            InputSource inputSource = _inputSourceRef.get();
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setIgnoringComments(true);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document document = db.parse(inputSource);
                loadSchema(document);
            } catch (Exception e) {
                throw new MetaModelException("Error parsing XML file: " + e.getMessage(), e);
            }
        }
        return this;
    }

    private void loadSchema(Document document) {
        Element rootElement = document.getDocumentElement();
        loadTables(rootElement, "", null, 0);

        // Remove tables from schema that has no data (typically root
        // node or pure XML structure)
        Table[] tables = _schema.getTables();
        for (Table table : tables) {
            String tableName = table.getName();
            List<Object[]> tableRows = _tableData.get(tableName);
            if (tableRows == null) {
                logger.info("Remove table (no data in it): {}", tableName);
                _schema.removeTable(table);
            } else {
                // Rename all ID columns to reasonable names (preferably
                // "id")
                MutableColumn idColumn = getIdColumn((MutableTable) table);
                MutableColumn column = (MutableColumn) table.getColumnByName("id");
                if (column == null) {
                    idColumn.setName("id");
                }

                // Remove text content column, if it is never populated
                MutableColumn textContentColumn = (MutableColumn) getTextContentColumn((MutableTable) table, null);
                int textContentColumnIndex = textContentColumn.getColumnNumber();
                boolean found = false;
                for (Object[] objects : tableRows) {
                    if (objects[textContentColumnIndex] != null) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    ((MutableTable) table).removeColumn(textContentColumn);
                } else {
                    // Rename all text content columns to reasonable
                    // names (preferably element node name)
                    String currentName = textContentColumn.getName();
                    String preferredName = currentName.substring(0, currentName.length() - TEXT_CONTENT_TEMP_SUFFIX
                            .length());
                    column = (MutableColumn) table.getColumnByName(preferredName);
                    if (column == null) {
                        textContentColumn.setName(preferredName);
                    }
                }
            }
        }
        if (_autoFlattenTables) {
            autoFlattenTables();
        }
    }

    private void loadTables(Element element, String tablePrefix, Column parentKeyColumn, int parentKey) {
        Attr[] attributes = getAttributes(element);
        String textContent = getTextContent(element);
        String tableName = tablePrefix + element.getNodeName();
        if (attributes.length > 0 || textContent != null || hasSiblings(element)) {
            // We need to represent this type of node with a table
            MutableTable table = (MutableTable) _schema.getTableByName(tableName);
            Column idColumn;
            MutableColumn foreignKeyColumn;
            List<Object[]> tableRows;
            if (table == null) {
                logger.info("Creating table: {}", tableName);
                table = new MutableTable(tableName, TableType.TABLE, _schema);
                _schema.addTable(table);
                idColumn = getIdColumn(table);
                tableRows = new ArrayList<Object[]>();
                _tableData.put(tableName, tableRows);

                if (parentKeyColumn != null) {
                    Table parentTable = parentKeyColumn.getTable();
                    foreignKeyColumn = new MutableColumn(parentTable.getName() + "_id", parentKeyColumn.getType(),
                            table, table.getColumnCount(), false);
                    foreignKeyColumn.setNativeType(NATIVE_TYPE_FOREIGN_KEY);
                    table.addColumn(foreignKeyColumn);

                    MutableRelationship.createRelationship(new Column[] { parentKeyColumn }, new Column[] {
                            foreignKeyColumn });

                } else {
                    foreignKeyColumn = null;
                }
            } else {
                idColumn = getIdColumn(table);
                tableRows = _tableData.get(tableName);
                Column[] foreignKeys = table.getForeignKeys();
                if (foreignKeys.length == 1) {
                    foreignKeyColumn = (MutableColumn) foreignKeys[0];
                } else {
                    foreignKeyColumn = null;
                }
            }

            Column textContentColumn = getTextContentColumn(table, element.getNodeName());
            Map<Column, String> columnValues = new HashMap<Column, String>();
            for (Attr attr : attributes) {
                String name = attr.getName();
                MutableColumn column = (MutableColumn) table.getColumnByName(name);
                if (column == null) {
                    logger.info("Creating column: {}.{}", tableName, name);
                    column = new MutableColumn(name, ColumnType.STRING, table, table.getColumnCount(), true);
                    column.setNativeType(NATIVE_TYPE_ATTRIBUTE);
                    table.addColumn(column);
                }
                columnValues.put(column, attr.getValue());
            }

            // Create a row
            Object[] rowData = new Object[table.getColumnCount()];
            // Iterate id column
            int id = tableRows.size() + 1;
            rowData[idColumn.getColumnNumber()] = id;
            if (foreignKeyColumn != null) {
                rowData[foreignKeyColumn.getColumnNumber()] = parentKey;
            }
            // Add value for text content (if available)
            if (textContent != null) {
                rowData[textContentColumn.getColumnNumber()] = textContent;
            }
            // Add values for attributes
            for (Entry<Column, String> entry : columnValues.entrySet()) {
                rowData[entry.getKey().getColumnNumber()] = entry.getValue();
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Adding data [{}] to table: {}", Arrays.toString(rowData), tableName);
            }

            if (!isRootElement(element)) {
                // Set the parent key column to this tables id column so
                // child tables can create relationship to it
                parentKey = id;
                parentKeyColumn = idColumn;
            }
            tableRows.add(rowData);
        }

        if (!isRootElement(element)) {
            tablePrefix = tableName + "_";
        }
        Element[] childElements = getChildElements(element);
        for (int i = 0; i < childElements.length; i++) {
            loadTables(childElements[i], tablePrefix, parentKeyColumn, parentKey);
        }
    }

    private Column getTextContentColumn(MutableTable table, String preferredColumnName) {
        Column[] columns = table.getColumns();
        MutableColumn column = null;
        for (Column col : columns) {
            if (NATIVE_TYPE_TEXT.equals(col.getNativeType())) {
                column = (MutableColumn) col;
                break;
            }
        }
        if (column == null && preferredColumnName != null) {
            logger.info("Creating text content column for table: " + table.getName());
            column = new MutableColumn(preferredColumnName + TEXT_CONTENT_TEMP_SUFFIX, ColumnType.STRING, table, table
                    .getColumnCount(), true);
            column.setNativeType(NATIVE_TYPE_TEXT);
            table.addColumn(column);
        }
        return column;
    }

    private MutableColumn getIdColumn(MutableTable table) {
        Column[] columns = table.getColumns();
        MutableColumn column = null;
        for (Column col : columns) {
            if (NATIVE_TYPE_PRIMARY_KEY.equals(col.getNativeType())) {
                column = (MutableColumn) col;
                break;
            }
        }
        if (column == null) {
            String tableName = table.getName();
            logger.info("Creating id column for table: " + tableName);
            column = new MutableColumn(tableName + "_metamodel_surrogate_id", ColumnType.INTEGER, table, table
                    .getColumnCount(), false);
            column.setNativeType(NATIVE_TYPE_PRIMARY_KEY);
            column.setIndexed(true);
            table.addColumn(column);
        }
        return column;
    }

    public static String getTextContent(Element element) {
        String textContent = null;
        NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof Text) {
                textContent = ((Text) node).getWholeText();
                break;
            }
        }
        if (textContent != null) {
            textContent = textContent.trim();
            if (!"".equals(textContent)) {
                return textContent;
            }
        }
        return null;
    }

    public static Attr[] getAttributes(Element element) {
        List<Attr> result = new ArrayList<Attr>();
        NamedNodeMap attributes = element.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Attr attribute = (Attr) attributes.item(i);
            result.add(attribute);
        }
        return result.toArray(new Attr[result.size()]);
    }

    public static boolean hasSiblings(Element element) {
        // Don't look for siblings when we are at the root element
        if (!isRootElement(element)) {
            String name = element.getNodeName();
            Element[] siblingNodes = getChildElements((Element) element.getParentNode());
            for (int i = 0; i < siblingNodes.length; i++) {
                Element siblingNode = siblingNodes[i];
                if (siblingNode != element && name.equals(siblingNode.getNodeName())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Element[] getChildElements(Element element) {
        List<Element> result = new ArrayList<Element>();
        NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child instanceof Element) {
                result.add((Element) child);
            }
        }
        return result.toArray(new Element[result.size()]);
    }

    public static boolean isRootElement(Element element) {
        return !(element.getParentNode() instanceof Element);
    }

    public XmlDomDataContext flattenTables(Relationship relationship) {
        MutableTable primaryTable = (MutableTable) relationship.getPrimaryTable();
        MutableTable foreignTable = (MutableTable) relationship.getForeignTable();

        // Check that foreignTable is not primary table in other relationships
        // (if so we can't flatten as that would require id-rewriting of those
        // foreign tables as well)
        if (foreignTable.getPrimaryKeyRelationships().length != 0) {
            Relationship[] foreignPrimaryRelationships = foreignTable.getPrimaryKeyRelationships();
            String[] foreignPrimaryNames = new String[foreignPrimaryRelationships.length];
            for (int i = 0; i < foreignPrimaryRelationships.length; i++) {
                foreignPrimaryNames[i] = foreignPrimaryRelationships[i].getForeignTable().getName();
            }
            throw new UnsupportedOperationException("Cannot flatten foreign table '" + foreignTable.getName()
                    + "' as it acts as primary table for tables: " + Arrays.toString(foreignPrimaryNames));
        }

        List<Column> primaryColumns = new ArrayList<Column>(Arrays.asList(primaryTable.getColumns()));
        List<Column> foreignColumns = new ArrayList<Column>(Arrays.asList(foreignTable.getColumns()));

        // Remove the surrogate id
        String primaryTableName = primaryTable.getName();
        String foreignTableName = foreignTable.getName();
        MutableColumn idColumn = getIdColumn(foreignTable);
        foreignColumns.remove(idColumn);

        // Remove the foreign keys
        Column[] foreignKeys = foreignTable.getForeignKeys();
        for (Column foreignKey : foreignKeys) {
            foreignColumns.remove(foreignKey);
        }

        Query q = new Query();
        q.select(primaryColumns.toArray(new Column[primaryColumns.size()]));
        q.select(foreignColumns.toArray(new Column[foreignColumns.size()]));
        q.from(new FromItem(JoinType.LEFT, relationship));
        if (logger.isDebugEnabled()) {
            logger.debug("Setting table data for '{}' to query result: {}", primaryTableName, q.toString());
        }
        List<Object[]> tableRows = executeQuery(q).toObjectArrays();

        for (Column foreignColumn : foreignColumns) {
            MutableColumn newPrimaryColumn = new MutableColumn(foreignColumn.getName(), foreignColumn.getType(),
                    primaryTable, primaryTable.getColumnCount(), foreignColumn.isNullable());
            newPrimaryColumn.setIndexed(foreignColumn.isIndexed());
            newPrimaryColumn.setNativeType(foreignColumn.getNativeType());
            primaryTable.addColumn(newPrimaryColumn);
        }
        _tableData.put(primaryTableName, tableRows);

        MutableSchema mutableSchema = (MutableSchema) foreignTable.getSchema();
        mutableSchema.removeTable(foreignTable);

        _tableData.remove(foreignTableName);
        ((MutableRelationship) relationship).remove();

        if (logger.isInfoEnabled()) {
            logger.info("Tables '" + primaryTableName + "' and '" + foreignTableName + "' flattened to: "
                    + primaryTableName);
            if (logger.isDebugEnabled()) {
                logger.debug(primaryTableName + " columns: " + Arrays.toString(primaryTable.getColumns()));
            }
        }

        return this;
    }

    /**
     * Automatically flattens tables that only contain a single data carrying
     * column. Data carrying column are all columns that are not artificial
     * columns (created to enable referential integrity between tag-to-table
     * mapped tables).
     */
    public XmlDomDataContext autoFlattenTables() {
        Table[] tables = _schema.getTables();
        for (Table table : tables) {
            // First check to see that this table still exist (ie. has not been
            // flattened in a previous loop)
            if (_tableData.containsKey(table.getName())) {
                // Find all tables that represent inner tags
                Relationship[] foreignKeyRelationships = table.getForeignKeyRelationships();
                if (foreignKeyRelationships.length == 1 && table.getPrimaryKeyRelationships().length == 0) {
                    Relationship foreignKeyRelationship = foreignKeyRelationships[0];

                    // If there is exactly one inner tag then we can probably
                    // flatten the tables, but it's only relevant if the inner
                    // tag only carry a single data column
                    int nonDataColumns = 0;
                    Column[] columns = table.getColumns();
                    for (Column column : columns) {
                        String nativeType = column.getNativeType();
                        // Use the native column type constants to determine if
                        // the column is an artificial column
                        if (NATIVE_TYPE_FOREIGN_KEY.equals(nativeType) || NATIVE_TYPE_PRIMARY_KEY.equals(nativeType)) {
                            nonDataColumns++;
                        }
                    }

                    if (columns.length == nonDataColumns + 1) {
                        // If the foreign key is unique for all rows, we will
                        // flatten it (otherwise it means that multiple inner
                        // tags occur, which requires two tables to deal with
                        // multiplicity)
                        boolean uniqueForeignKeys = true;

                        Column[] foreignColumns = foreignKeyRelationship.getForeignColumns();

                        SelectItem countAllItem = SelectItem.getCountAllItem();
                        Query q = new Query().select(foreignColumns).select(countAllItem).from(table).groupBy(
                                foreignColumns);
                        DataSet data = executeQuery(q);
                        Comparable<Object> comparable = NumberComparator.getComparable(1);
                        while (data.next()) {
                            Object value = data.getRow().getValue(countAllItem);
                            if (comparable.compareTo(value) < 0) {
                                // If the value is compared larger than 1, we
                                // have several inner tags
                                uniqueForeignKeys = false;
                                break;
                            }
                        }
                        data.close();

                        if (uniqueForeignKeys) {
                            flattenTables(foreignKeyRelationship);
                        }
                    }
                }
            }
        }
        return this;
    }
}