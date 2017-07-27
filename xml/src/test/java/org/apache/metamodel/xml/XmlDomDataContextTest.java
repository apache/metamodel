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
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import javax.swing.table.TableModel;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XmlDomDataContextTest extends TestCase {

    public void testGetFromUrl() throws Exception {
        // Retrieve a test file, but through URL from GitHub servers.
        URL url = new URL(
                "https://raw.githubusercontent.com/apache/metamodel/master/xml/src/test/resources/xml_input_eobjects.xml");
        try {
            XmlDomDataContext dataContext = new XmlDomDataContext(url, false);
            assertEquals("xml_input_eobjects.xml", dataContext.getDefaultSchema().getName());
            assertEquals(2, dataContext.getSchemaNames().size());
            Schema schema = dataContext.getSchemaByName("xml_input_eobjects.xml");
            assertEquals("Schema[name=xml_input_eobjects.xml]", schema.toString());
            assertEquals(5, schema.getTableCount());
        } catch (IllegalArgumentException e) {
            // If the network is not accessible omit the test
            if (!(e.getCause() instanceof UnknownHostException)) {
                throw e;
            }
        }
    }

    public void testGetSchemas() throws Exception {
        File file = new File("src/test/resources/xml_input_eobjects.xml");
        XmlDomDataContext dataContext = new XmlDomDataContext(file, false);

        assertEquals("xml_input_eobjects.xml", dataContext.getDefaultSchema().getName());
        assertEquals(2, dataContext.getSchemaNames().size());

        Schema schema = dataContext.getSchemaByName("xml_input_eobjects.xml");
        assertEquals("Schema[name=xml_input_eobjects.xml]", schema.toString());

        assertEquals(5, schema.getTableCount());
        assertEquals("[Table[name=eobjects.dk,type=TABLE,remarks=null], "
                + "Table[name=contributors_person,type=TABLE,remarks=null], "
                + "Table[name=contributors_person_name,type=TABLE,remarks=null], "
                + "Table[name=contributors_person_address,type=TABLE,remarks=null], "
                + "Table[name=projects_project,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));

        Table table = schema.getTableByName("eobjects.dk");
        assertEquals(2, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=website,columnNumber=2,type=STRING,nullable=true,nativeType=XML Attribute,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        table = schema.getTableByName("contributors_person");
        assertEquals(1, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        table = schema.getTableByName("contributors_person_name");
        assertEquals(3, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=contributors_person_id,columnNumber=1,type=INTEGER,nullable=false,nativeType=Auto-generated foreign key,columnSize=null], "
                        + "Column[name=name,columnNumber=2,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));
        assertEquals("[Relationship[primaryTable=contributors_person,primaryColumns=[id],"
                + "foreignTable=contributors_person_name,foreignColumns=[contributors_person_id]]]",
                Arrays.toString(table.getRelationships().toArray()));

        table = schema.getTableByName("contributors_person_address");
        assertEquals(3, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=contributors_person_id,columnNumber=1,type=INTEGER,nullable=false,nativeType=Auto-generated foreign key,columnSize=null], "
                        + "Column[name=address,columnNumber=2,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));
        assertEquals("[Relationship[primaryTable=contributors_person,primaryColumns=[id],"
                + "foreignTable=contributors_person_address,foreignColumns=[contributors_person_id]]]",
                Arrays.toString(table.getRelationships().toArray()));

        table = schema.getTableByName("projects_project");
        assertEquals(3, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=project,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=name,columnNumber=2,type=STRING,nullable=true,nativeType=XML Attribute,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        dataContext.autoFlattenTables();
        table = schema.getTableByName("contributors_person");
        assertEquals(2, table.getColumnCount());
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=name,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));
    }

    public void testExecuteQuery() throws Exception {
        File file = new File("src/test/resources/xml_input_eobjects.xml");
        DataContext dc = new XmlDomDataContext(file, false);
        Schema schema = dc.getDefaultSchema();
        Table table = schema.getTableByName("projects_project");
        Query q = new Query().select(table.getColumns()).from(table, "p");
        assertEquals("SELECT p.id, p.project, p.name FROM xml_input_eobjects.xml.projects_project p", q.toString());
        DataSet data = dc.executeQuery(q);
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(3, tableModel.getColumnCount());
        assertEquals(2, tableModel.getRowCount());

        // ID
        assertEquals(1, tableModel.getValueAt(0, 0));
        assertEquals(2, tableModel.getValueAt(1, 0));

        // Project (text content)
        assertEquals("Some description", tableModel.getValueAt(0, 1));
        assertNull(tableModel.getValueAt(1, 1));

        // Name
        assertEquals("datacleaner", tableModel.getValueAt(0, 2));
        assertEquals("MetaModel", tableModel.getValueAt(1, 2));

        // Make a new query that joins the normalized tables together
        table = schema.getTableByName("contributors_person_address");
        Relationship relationShip = table.getRelationships().iterator().next();
        q = new Query().select(relationShip.getPrimaryTable().getColumns())
                .select(relationShip.getForeignTable().getColumns()).from(new FromItem(JoinType.INNER, relationShip));

        assertEquals(
                "SELECT contributors_person.id, contributors_person_address.id, "
                        + "contributors_person_address.contributors_person_id, contributors_person_address.address "
                        + "FROM xml_input_eobjects.xml.contributors_person INNER JOIN xml_input_eobjects.xml.contributors_person_address "
                        + "ON contributors_person.id = contributors_person_address.contributors_person_id",
                q.toString());

        data = dc.executeQuery(q);
        tableModel = new DataSetTableModel(data);
        assertEquals(4, tableModel.getColumnCount());
        assertEquals(4, tableModel.getRowCount());

        assertEquals("1", tableModel.getValueAt(0, 0).toString());
        assertEquals("1", tableModel.getValueAt(0, 1).toString());
        assertEquals("1", tableModel.getValueAt(0, 2).toString());
        assertEquals("My address", tableModel.getValueAt(0, 3).toString());

        assertEquals("1", tableModel.getValueAt(1, 0).toString());
        assertEquals("2", tableModel.getValueAt(1, 1).toString());
        assertEquals("1", tableModel.getValueAt(1, 2).toString());
        assertEquals("Another address", tableModel.getValueAt(1, 3).toString());

        assertEquals("1", tableModel.getValueAt(2, 0).toString());
        assertEquals("3", tableModel.getValueAt(2, 1).toString());
        assertEquals("1", tableModel.getValueAt(2, 2).toString());
        assertEquals("A third address", tableModel.getValueAt(2, 3).toString());

        assertEquals("2", tableModel.getValueAt(3, 0).toString());
        assertEquals("4", tableModel.getValueAt(3, 1).toString());
        assertEquals("2", tableModel.getValueAt(3, 2).toString());
        assertEquals("Asbjorns address", tableModel.getValueAt(3, 3).toString());
    }

    public void testGetTextContent() throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setIgnoringComments(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(new File("src/test/resources/xml_input_simple.xml"));
        Element rootElement = document.getDocumentElement();
        Element[] childElements = XmlDomDataContext.getChildElements(rootElement);
        assertEquals("w00p", XmlDomDataContext.getTextContent(childElements[0]));
        assertNull(XmlDomDataContext.getTextContent(childElements[1]));
        assertNull(XmlDomDataContext.getTextContent(childElements[2]));
        assertNull(XmlDomDataContext.getTextContent(rootElement));

        XmlDomDataContext strategy = new XmlDomDataContext("foobarSchema", document, true);
        Schema schema = strategy.getSchemaByName("foobarSchema");
        assertEquals("Schema[name=foobarSchema]", schema.toString());
        assertEquals("[Table[name=child,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));
        Table table = schema.getTable(0);
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=child,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));
    }

    public void testFlattenTables() throws Exception {
        File file = new File("src/test/resources/xml_input_flatten_tables.xml");
        XmlDomDataContext dc = new XmlDomDataContext(file, false);
        Schema schema = dc.getDefaultSchema();
        assertEquals("[Table[name=dependency,type=TABLE,remarks=null], "
                + "Table[name=dependency_groupId,type=TABLE,remarks=null], "
                + "Table[name=dependency_artifactId,type=TABLE,remarks=null], "
                + "Table[name=dependency_version,type=TABLE,remarks=null], "
                + "Table[name=dependency_scope,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));
        Table dependencyTable = schema.getTableByName("dependency");
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null]]",
                Arrays.toString(dependencyTable.getColumns().toArray()));
        List<Object[]> dependencyData = dc
                .materializeMainSchemaTable(dependencyTable, dependencyTable.getColumns(), -1).toObjectArrays();
        assertEquals(11, dependencyData.size());
        assertEquals("[1]", Arrays.toString(dependencyData.get(0)));
        assertEquals("[11]", Arrays.toString(dependencyData.get(10)));

        Relationship relationship = schema.getTableByName("dependency_groupId").getRelationships().iterator().next();
        assertEquals(
                "Relationship[primaryTable=dependency,primaryColumns=[id],foreignTable=dependency_groupId,foreignColumns=[dependency_id]]",
                relationship.toString());

        dc.flattenTables(relationship);

        assertEquals("[Table[name=dependency,type=TABLE,remarks=null], "
                + "Table[name=dependency_artifactId,type=TABLE,remarks=null], "
                + "Table[name=dependency_version,type=TABLE,remarks=null], "
                + "Table[name=dependency_scope,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));

        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=groupId,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(dependencyTable.getColumns().toArray()));

        dependencyData = dc.materializeMainSchemaTable(dependencyTable, dependencyTable.getColumns(), -1)
                .toObjectArrays();
        assertEquals(11, dependencyData.size());
        assertEquals("[1, joda-time]", Arrays.toString(dependencyData.get(0)));
        assertEquals("[11, mysql]", Arrays.toString(dependencyData.get(10)));

        dc.autoFlattenTables();

        assertEquals("[Table[name=dependency,type=TABLE,remarks=null]]", Arrays.toString(schema.getTables().toArray()));
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=groupId,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=artifactId,columnNumber=2,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=version,columnNumber=3,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=scope,columnNumber=4,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(dependencyTable.getColumns().toArray()));

        dependencyData = dc.materializeMainSchemaTable(dependencyTable, dependencyTable.getColumns(), -1)
                .toObjectArrays();
        assertEquals(11, dependencyData.size());
        assertEquals("[1, joda-time, joda-time, 1.5.2, compile]", Arrays.toString(dependencyData.get(0)));
        assertEquals("[11, mysql, mysql-connector-java, 5.1.6, test]", Arrays.toString(dependencyData.get(10)));
    }

    public void testParsePom() throws Exception {
        XmlDomDataContext dc = new XmlDomDataContext(new File("src/test/resources/xml_input_pom.xml"));
        Schema schema = dc.getDefaultSchema();

        Table table = schema.getTableByName("dependencies_dependency");
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=groupId,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=artifactId,columnNumber=2,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=version,columnNumber=3,type=STRING,nullable=true,nativeType=XML Text,columnSize=null], "
                        + "Column[name=scope,columnNumber=4,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        table = schema.getTableByName("inceptionYear");
        assertEquals(
                "[Column[name=id,columnNumber=0,type=INTEGER,nullable=false,nativeType=Auto-generated primary key,columnSize=null], "
                        + "Column[name=inceptionYear,columnNumber=1,type=STRING,nullable=true,nativeType=XML Text,columnSize=null]]",
                Arrays.toString(table.getColumns().toArray()));

        // first read
        DataSet data = dc.executeQuery(new Query().select(table.getColumnByName("inceptionYear")).from(table)
                .setMaxRows(1));
        assertTrue(data.next());
        assertEquals("2007", data.getRow().getValue(0));
        assertFalse(data.next());

        // repeated read
        data = dc.query().from(table).select("inceptionYear").execute();
        assertTrue(data.next());
        assertEquals("2007", data.getRow().getValue(0));
        assertFalse(data.next());
    }

    public void testAutoflattenTicket229() throws Exception {
        DataContext dc = new XmlDomDataContext(new File("src/test/resources/xml_input_ticket_229.xml"));
        Schema schema = dc.getDefaultSchema();
        assertNotNull(schema);

        assertEquals("[VirtualBox, Machine, Machine_ExtraData_ExtraDataItem, Machine_Hardware_Boot_Order, "
                + "Machine_Hardware_Display, Machine_Hardware_RemoteDisplay, Machine_Hardware_BIOS_Logo, "
                + "Machine_Hardware_DVDDrive, Machine_Hardware_USBController, Machine_Hardware_SATAController, "
                + "Machine_Hardware_Network_Adapter, Machine_Hardware_UART_Port, Machine_Hardware_LPT_Port, "
                + "Machine_Hardware_AudioAdapter, Machine_Hardware_SharedFolders_SharedFolder, "
                + "Machine_Hardware_Guest, Machine_HardDiskAttachments_HardDiskAttachment]",
                Arrays.toString(schema.getTableNames().toArray()));
        assertEquals(
                "[id, OSType, lastStateChange, name, uuid, enabled, enabled, RAMSize, enabled, enabled, mode, value, "
                        + "enabled, type, enabled, mode]",
                Arrays.toString(schema.getTableByName("Machine").getColumnNames().toArray()));
    }
}