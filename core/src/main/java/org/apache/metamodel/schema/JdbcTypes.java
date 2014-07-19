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

/**
 * This is a copy of the content (comments removed) of Java 6.0's
 * java.sql.Types. It is backwards compatible with older versions, but have
 * additional types (confirmed by JavaTypesTest). It is being used to convert
 * JDBC types to ColumnType enumerations.
 */
public final class JdbcTypes {

	// Prevent instantiation
	private JdbcTypes() {
	}

	public final static int BIT = -7;
	public final static int TINYINT = -6;
	public final static int SMALLINT = 5;
	public final static int INTEGER = 4;
	public final static int BIGINT = -5;
	public final static int FLOAT = 6;
	public final static int REAL = 7;
	public final static int DOUBLE = 8;
	public final static int NUMERIC = 2;
	public final static int DECIMAL = 3;
	public final static int CHAR = 1;
	public final static int VARCHAR = 12;
	public final static int LONGVARCHAR = -1;
	public final static int DATE = 91;
	public final static int TIME = 92;
	public final static int TIMESTAMP = 93;
	public final static int BINARY = -2;
	public final static int VARBINARY = -3;
	public final static int LONGVARBINARY = -4;
	public final static int NULL = 0;
	public final static int OTHER = 1111;
	public final static int JAVA_OBJECT = 2000;
	public final static int DISTINCT = 2001;
	public final static int STRUCT = 2002;
	public final static int ARRAY = 2003;
	public final static int BLOB = 2004;
	public final static int CLOB = 2005;
	public final static int REF = 2006;
	public final static int DATALINK = 70;
	public final static int BOOLEAN = 16;
	public final static int ROWID = -8;
	public static final int NCHAR = -15;
	public static final int NVARCHAR = -9;
	public static final int LONGNVARCHAR = -16;
	public static final int NCLOB = 2011;
	public static final int SQLXML = 2009;
}