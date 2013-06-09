/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel;

import java.io.File;

import org.eobjects.metamodel.excel.ExcelConfiguration;
import org.eobjects.metamodel.excel.ExcelDataContext;

import junit.framework.TestCase;

public class DataContextFactoryTest extends TestCase {

    public void testCreateExcelDataContext() throws Exception {
        File file = new File("../excel/src/test/resources/xls_people.xls");
        assertTrue(file.exists());

        UpdateableDataContext dc;

        dc = DataContextFactory.createExcelDataContext(file);
        assertNotNull(dc);
        assertTrue(dc instanceof ExcelDataContext);

        dc = DataContextFactory.createExcelDataContext(file,
                new ExcelConfiguration());
        assertNotNull(dc);
        assertTrue(dc instanceof ExcelDataContext);
    }

}
