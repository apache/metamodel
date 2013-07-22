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
package org.apache.metamodel.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.logging.LogManager;

import junit.framework.TestCase;

import org.apache.metamodel.MetaModelException;

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
