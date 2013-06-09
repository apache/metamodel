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
package org.eobjects.metamodel.dbmains;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

public class PostgresqlMain {

	private static final String CONNECTION_STRING = "jdbc:postgresql://localhost/dellstore2";
	private static final String USERNAME = "eobjects";
	private static final String PASSWORD = "eobjects";

	/**
	 * @param args
	 */ 
	public static void main(String[] args) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(CONNECTION_STRING,
					USERNAME, PASSWORD);

			JdbcDataContext dc = new JdbcDataContext(connection);
			final Schema schema = dc.getDefaultSchema();
			dc.executeUpdate(new UpdateScript() {
				@Override
				public void run(UpdateCallback cb) {
					Table table = cb.createTable(schema, "my_table")
							.withColumn("id").ofType(ColumnType.INTEGER)
							.ofNativeType("SERIAL").nullable(false)
							.withColumn("person name").ofSize(255)
							.withColumn("age").ofType(ColumnType.INTEGER)
							.execute();

					for (int i = 0; i < 1000000; i++) {
						cb.insertInto(table).value("person name", "John Doe")
								.value("age", i + 10).execute();
					}

				}
			});

			Table table = schema.getTableByName("my_table");
			Query query = dc.query().from(table).selectCount().toQuery();
			DataSet ds = dc.executeQuery(query);
			ds.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.createStatement().execute("DROP TABLE my_table");
				}
			} catch (SQLException e) {
				throw new MetaModelException(
						"Failed to execute INSERT statement", e);
			}
		}

	}

}
