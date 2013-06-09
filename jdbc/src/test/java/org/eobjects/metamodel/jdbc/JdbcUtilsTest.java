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
package org.eobjects.metamodel.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.logging.LogManager;

import junit.framework.TestCase;

import org.eobjects.metamodel.MetaModelException;

public class JdbcUtilsTest extends TestCase {

	public void testConvertToMetaModelException() throws Exception {
		LogManager logManager = LogManager.getLogManager();
		File logConfigFile = new File("src/test/resources/logconfig.txt");
		assertTrue(logConfigFile.exists());
		logManager.readConfiguration(new FileInputStream(logConfigFile));

		assertTrue(JdbcUtils.wrapException(new SQLException("msg"), "foo") instanceof MetaModelException);
		assertTrue(JdbcUtils.wrapException(
				new SQLException("msg", "sql state"), "foo") instanceof MetaModelException);
		assertTrue(JdbcUtils.wrapException(new SQLException("msg", "sql state",
				40), "foo") instanceof MetaModelException);

		SQLException exceptionWithNext = new SQLException("msg", "sql state",
				41);
		exceptionWithNext.setNextException(new SQLException("i am next"));
		assertTrue(JdbcUtils.wrapException(exceptionWithNext, "foo") instanceof MetaModelException);
	}
}
