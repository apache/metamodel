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
package org.apache.metamodel.schema;

import java.util.Arrays;

import org.apache.metamodel.MetaModelTestCase;

public class SchemaModelTest extends MetaModelTestCase {

    public void testGetExampleSchema() throws Exception {
        Schema schema = getExampleSchema();
        assertEquals("MetaModelSchema", schema.getName());
        assertEquals("Schema[name=MetaModelSchema]", schema.toString());
        assertEquals(5, schema.getRelationships().length);

        assertEquals(4, schema.getTableCount());
        assertEquals(3, schema.getTableCount(TableType.TABLE));
        assertEquals(1, schema.getTableCount(TableType.VIEW));

        assertNull(schema.getTableByName("foobar"));
        assertNull(schema.getTableByName(null));

        Table contributorTable = schema.getTableByName(TABLE_CONTRIBUTOR);
        assertEquals(3, contributorTable.getColumnCount());
        assertEquals(2, contributorTable.getRelationshipCount());

        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        assertEquals(4, projectTable.getColumnCount());
        assertEquals(2, projectTable.getRelationshipCount());
        assertNotNull(projectTable.getColumnByName("project_id"));

        assertEquals("[project_id, name, lines_of_code, parent_project_id]",
                Arrays.toString(projectTable.getColumnNames()));

        assertEquals(
                "[Column[name=project_id,columnNumber=0,type=INTEGER,nullable=false,nativeType=null,columnSize=null], "
                        + "Column[name=lines_of_code,columnNumber=2,type=BIGINT,nullable=true,nativeType=null,columnSize=null], "
                        + "Column[name=parent_project_id,columnNumber=3,type=INTEGER,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(projectTable.getNumberColumns()));

        assertEquals("[Column[name=name,columnNumber=1,type=VARCHAR,nullable=false,nativeType=null,columnSize=null]]",
                Arrays.toString(projectTable.getLiteralColumns()));

        assertEquals("[]", Arrays.toString(projectTable.getTimeBasedColumns()));

        assertNull(projectTable.getColumnByName("foobar"));
        assertNull(projectTable.getColumnByName(null));

        Table roleTable = schema.getTableByName(TABLE_ROLE);
        assertEquals(3, roleTable.getColumnCount());
        assertEquals(3, roleTable.getRelationshipCount());

        Table projectContributorView = schema.getTableByName(TABLE_PROJECT_CONTRIBUTOR);
        assertEquals(3, projectContributorView.getColumnCount());
        assertEquals(3, projectContributorView.getRelationshipCount());

        Relationship[] projectContributorToContributorRelations = projectContributorView
                .getRelationships(contributorTable);
        assertEquals(1, projectContributorToContributorRelations.length);
        Relationship[] contributorToProjectContributorRelations = contributorTable
                .getRelationships(projectContributorView);
        assertEquals(1, contributorToProjectContributorRelations.length);
        assertTrue(Arrays.equals(projectContributorToContributorRelations, contributorToProjectContributorRelations));

        assertEquals(
                "Relationship[primaryTable=contributor,primaryColumns=[name],foreignTable=project_contributor,foreignColumns=[contributor]]",
                projectContributorToContributorRelations[0].toString());

        ((MutableRelationship) projectContributorToContributorRelations[0]).remove();
        projectContributorToContributorRelations = projectContributorView.getRelationships(contributorTable);
        assertEquals(0, projectContributorToContributorRelations.length);
        contributorToProjectContributorRelations = contributorTable.getRelationships(projectContributorView);
        assertEquals(0, contributorToProjectContributorRelations.length);

        // Get primary keys / Get foreign keys test
        assertEquals(
                "[Column[name=contributor_id,columnNumber=0,type=INTEGER,nullable=false,nativeType=null,columnSize=null]]",
                Arrays.toString(contributorTable.getPrimaryKeys()));
        assertEquals("[]", Arrays.toString(contributorTable.getForeignKeys()));

        assertEquals(
                "[Column[name=contributor_id,columnNumber=0,type=INTEGER,nullable=false,nativeType=null,columnSize=null], Column[name=project_id,columnNumber=1,type=INTEGER,nullable=false,nativeType=null,columnSize=null]]",
                Arrays.toString(roleTable.getPrimaryKeys()));
        Column[] foreignKeys = roleTable.getForeignKeys();
        assertEquals(2, foreignKeys.length);
    }
}