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

package org.eobjects.metamodel.schema;

/**
 * This is a copy of the content (comments removed) of Java 6.0's
 * java.sql.Types. It is backwards compatible with older versions, but have
 * additional types (confirmed by JavaTypesTest). It is being used to convert
 * JDBC types to ColumnType enumerations.
 */
final class JdbcTypes {

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