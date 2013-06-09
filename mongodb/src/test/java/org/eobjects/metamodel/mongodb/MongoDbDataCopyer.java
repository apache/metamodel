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
package org.eobjects.metamodel.mongodb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileHelper;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Simple example program that can copy data to a MongoDB collection
 * 
 * @author Kasper Sørensen
 */
public class MongoDbDataCopyer {

    private final DataContext _sourceDataContext;
    private final DB _mongoDb;
    private final String _collectionName;
    private final String _sourceSchemaName;
    private final String _sourceTableName;

    // example copy job that will populate the mongodb with Derby data
    public static void main(String[] args) throws Exception {
        System.setProperty("derby.storage.tempDirector", FileHelper.getTempDir().getAbsolutePath());
        System.setProperty("derby.stream.error.file", File.createTempFile("metamodel-derby", ".log").getAbsolutePath());

        File dbFile = new File("../jdbc/src/test/resources/derby_testdb.jar");
        dbFile = dbFile.getCanonicalFile();
        if (!dbFile.exists()) {
            throw new IllegalStateException("File does not exist: " + dbFile);
        }

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection connection = DriverManager.getConnection("jdbc:derby:jar:(" + dbFile.getAbsolutePath()
                + ")derby_testdb;territory=en");
        connection.setReadOnly(true);

        DB db = new Mongo().getDB("orderdb_copy");

        DataContext sourceDataContext = new JdbcDataContext(connection);

        new MongoDbDataCopyer(db, "orders", sourceDataContext, "APP", "orders").copy();
        new MongoDbDataCopyer(db, "offices", sourceDataContext, "APP", "offices").copy();
        new MongoDbDataCopyer(db, "payments", sourceDataContext, "APP", "payments").copy();
        new MongoDbDataCopyer(db, "orderfact", sourceDataContext, "APP", "orderfact").copy();
        new MongoDbDataCopyer(db, "products", sourceDataContext, "APP", "products").copy();

        connection.close();
    }

    public MongoDbDataCopyer(DB mongoDb, String collectionName, DataContext sourceDataContext, String sourceSchemaName,
            String sourceTableName) {
        _mongoDb = mongoDb;
        _collectionName = collectionName;
        _sourceDataContext = sourceDataContext;
        _sourceSchemaName = sourceSchemaName;
        _sourceTableName = sourceTableName;
    }

    public void copy() {
        final MongoDbDataContext targetDataContext = new MongoDbDataContext(_mongoDb);
        targetDataContext.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                final Table sourceTable = getSourceTable();
                final Table targetTable = callback.createTable(targetDataContext.getDefaultSchema(), _collectionName)
                        .like(sourceTable).execute();
                final Column[] sourceColumns = sourceTable.getColumns();
                final DataSet dataSet = _sourceDataContext.query().from(sourceTable).select(sourceColumns).execute();
                while (dataSet.next()) {
                    final Row row = dataSet.getRow();

                    RowInsertionBuilder insertBuilder = callback.insertInto(targetTable);
                    for (Column column : sourceColumns) {
                        insertBuilder = insertBuilder.value(column.getName(), row.getValue(column));
                    }
                    insertBuilder.execute();
                }
                dataSet.close();
            }
        });
    }

    private Table getSourceTable() {
        final Schema schema;
        if (_sourceSchemaName != null) {
            schema = _sourceDataContext.getSchemaByName(_sourceSchemaName);
        } else {
            schema = _sourceDataContext.getDefaultSchema();
        }

        return schema.getTableByName(_sourceTableName);
    }
}
