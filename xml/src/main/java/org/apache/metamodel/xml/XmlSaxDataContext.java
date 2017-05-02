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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.RowPublisher;
import org.apache.metamodel.data.RowPublisherDataSet;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ImmutableSchema;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * XML datacontext which uses SAX parsing for fast and memory effecient reading
 * of XML files.
 * 
 * The DataContext requires the user to specify a set of (simplified) XPaths to
 * define which elements are row delimitors and which elements or attributes are
 * value/column definitions.
 */
public class XmlSaxDataContext extends QueryPostprocessDataContext {

    private static final Logger logger = LoggerFactory.getLogger(XmlSaxDataContext.class);

    public static final String COLUMN_NAME_ROW_ID = "row_id";

    private final Supplier<InputSource> _inputSourceRef;
    private final Map<XmlSaxTableDef, Map<String, String>> _valueXpaths;
    private String _schemaName;
    private XmlSaxTableDef[] _tableDefs;

    /**
     * Constructs an XML DataContext based on SAX parsing.
     * 
     * @param inputSourceRef
     *            a factory reference for the input source to read the XML from.
     *            The ref will be repeatedly called for each access to the file!
     * @param tableDefs
     *            an array of table definitions, which provide instructions as
     *            to the xpaths to apply to the document.
     * 
     * @see XmlSaxTableDef
     */
    public XmlSaxDataContext(Supplier<InputSource> inputSourceRef, XmlSaxTableDef... tableDefs) {
        _inputSourceRef = inputSourceRef;
        _tableDefs = tableDefs;
        _valueXpaths = new HashMap<XmlSaxTableDef, Map<String, String>>();
        _schemaName = null;

        for (XmlSaxTableDef tableDef : tableDefs) {
            LinkedHashMap<String, String> xpathMap = new LinkedHashMap<String, String>();
            _valueXpaths.put(tableDef, xpathMap);
            String[] valueXpaths = tableDef.getValueXpaths();
            for (String valueXpath : valueXpaths) {
                xpathMap.put(getName(tableDef, valueXpath), valueXpath);
            }
        }
    }

    public XmlSaxDataContext(final Resource resource, XmlSaxTableDef... tableDefs) {
        this(createInputSourceRef(resource), tableDefs);
    }

    public XmlSaxDataContext(final File file, XmlSaxTableDef... tableDefs) {
        this(createInputSourceRef(new FileResource(file)), tableDefs);
    }

    private static Supplier<InputSource> createInputSourceRef(final Resource resource) {
        return () -> {
            final InputStream in = resource.read();
            return new InputSource(in);
        };
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(getMainSchemaName());

        for (XmlSaxTableDef tableDef : _tableDefs) {
            final String rowXpath = tableDef.getRowXpath();
            final MutableTable table = new MutableTable(getTableName(tableDef)).setSchema(schema).setRemarks("XPath: "
                    + rowXpath);

            final MutableColumn rowIndexColumn = new MutableColumn(COLUMN_NAME_ROW_ID, ColumnType.INTEGER)
                    .setColumnNumber(0).setNullable(false).setTable(table).setRemarks("Row/tag index (0-based)");
            table.addColumn(rowIndexColumn);

            for (String valueXpath : tableDef.getValueXpaths()) {
                final MutableColumn column = new MutableColumn(getName(tableDef, valueXpath)).setRemarks("XPath: "
                        + valueXpath);
                if (valueXpath.startsWith("index(") && valueXpath.endsWith(")")) {
                    column.setType(ColumnType.INTEGER);
                } else {
                    column.setType(ColumnType.STRING);
                }
                column.setTable(table);
                table.addColumn(column);
            }
            schema.addTable(table);
        }

        return new ImmutableSchema(schema);
    }

    private XmlSaxTableDef getTableDef(Table table) {
        for (XmlSaxTableDef tableDef : _tableDefs) {
            if (getTableName(tableDef).equals(table.getName())) {
                return tableDef;
            }
        }
        throw new IllegalArgumentException("No table def found for table " + table);
    }

    private String getTableName(XmlSaxTableDef tableDef) {
        String xpath = tableDef.getRowXpath();
        int lastIndexOf = xpath.lastIndexOf('/');
        if (lastIndexOf != -1) {
            xpath = xpath.substring(lastIndexOf);
        }
        return xpath;
    }

    private String getName(XmlSaxTableDef tableDef, String xpath) {
        String rowXpath = tableDef.getRowXpath();
        if (xpath.startsWith(rowXpath)) {
            xpath = xpath.substring(rowXpath.length());
        }
        return xpath;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        if (_schemaName == null) {
            // when querying the schema name for the first time, pick the first
            // element of the document.
            try {
                SAXParserFactory saxFactory = SAXParserFactory.newInstance();
                SAXParser saxParser = saxFactory.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                xmlReader.setContentHandler(new DefaultHandler() {
                    @Override
                    public void startElement(String uri, String localName, String qName, Attributes attributes)
                            throws SAXException {
                        if (qName != null && qName.length() > 0) {
                            _schemaName = '/' + qName;
                            throw new XmlStopParsingException();
                        }
                    }
                });
                xmlReader.parse(_inputSourceRef.get());
            } catch (XmlStopParsingException e) {
                logger.debug("Parsing stop signal thrown");
            } catch (Exception e) {
                logger.error("Unexpected error occurred while retrieving schema name", e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new IllegalStateException(e);
            }
        }
        return _schemaName;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final XmlSaxTableDef tableDef = getTableDef(table);

        final String[] valueXpaths = new String[columns.length];
        final SelectItem[] selectItems = new SelectItem[columns.length];
        for (int i = 0; i < columns.length; i++) {
            final Column column = columns[i];
            selectItems[i] = new SelectItem(column);
            valueXpaths[i] = getXpath(tableDef, column);
        }

        final Action<RowPublisher> rowPublisherAction = new Action<RowPublisher>() {
            @Override
            public void run(RowPublisher rowPublisher) throws Exception {
                SAXParserFactory saxFactory = SAXParserFactory.newInstance();
                SAXParser saxParser = saxFactory.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                xmlReader.setContentHandler(new XmlSaxContentHandler(tableDef.getRowXpath(), rowPublisher,
                        valueXpaths));
                try {
                    xmlReader.parse(_inputSourceRef.get());
                } catch (XmlStopParsingException e) {
                    logger.debug("Parsing stop signal thrown");
                } catch (Exception e) {
                    logger.warn("Unexpected error occurred while parsing", e);
                    throw e;
                } finally {
                    rowPublisher.finished();
                }
            }
        };
        return new RowPublisherDataSet(selectItems, maxRows, rowPublisherAction);
    }

    private String getXpath(XmlSaxTableDef tableDef, Column column) {
        String columnName = column.getName();
        if (COLUMN_NAME_ROW_ID.equals(columnName)) {
            return "index(" + tableDef.getRowXpath() + ")";
        }
        String result = _valueXpaths.get(tableDef).get(columnName);
        if (result == null) {
            return columnName;
        }
        return result;
    }
}
