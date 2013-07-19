package org.eobjects.metamodel;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import javax.swing.table.TableModel;

import junit.framework.TestCase;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetTableModel;
import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.jdbc.JdbcTestTemplates;
import org.eobjects.metamodel.jdbc.QuerySplitter;
import org.eobjects.metamodel.jdbc.dialects.MysqlQueryRewriter;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;

/**
 * Test case that tests mysql interaction. The test requires the "sakila" sample
 * database that can be found at dev.mysql.com.
 * 
 * @see http://dev.mysql.com/doc/sakila/en/sakila.html#sakila-installation
 */
public class MysqlTest extends TestCase {

	private static final String CONNECTION_STRING = "jdbc:mysql://localhost/sakila?defaultFetchSize=" + Integer.MIN_VALUE;
	private static final String USERNAME = "eobjects";
	private static final String PASSWORD = "eobjects";
	private Connection _connection;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Class.forName("com.mysql.jdbc.Driver");
		_connection = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD);
	}
	
    public void testInterpretationOfNull() throws Exception {
        JdbcTestTemplates.interpretationOfNulls(_connection);
    }

	public void testDatabaseProductName() throws Exception {
		String databaseProductName = _connection.getMetaData().getDatabaseProductName();
		assertEquals(JdbcDataContext.DATABASE_PRODUCT_MYSQL, databaseProductName);
	}

	public void testAutomaticConversionWhenInsertingString() throws Exception {
		assertNotNull(_connection);

		Statement st = _connection.createStatement();
		try {
			// clean up, if nescesary
			st.execute("DROP TABLE test_table");
			st.close();
		} catch (SQLException e) {
			// do nothing
		}

		assertFalse(_connection.isReadOnly());

		JdbcDataContext dc = new JdbcDataContext(_connection);
		final Schema schema = dc.getDefaultSchema();
		assertEquals("sakila", schema.getName());

		dc.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback cb) {
				Table table = cb.createTable(schema, "test_table").withColumn("id").ofType(ColumnType.INTEGER)
						.asPrimaryKey().withColumn("birthdate").ofType(ColumnType.DATE).execute();

				cb.insertInto(table).value("id", "1").execute();
				cb.insertInto(table).value("id", 2).value("birthdate", "2011-12-21").execute();
			}
		});

		assertTrue(dc.getColumnByQualifiedLabel("test_table.id").isPrimaryKey());
		assertFalse(dc.getColumnByQualifiedLabel("test_table.birthdate").isPrimaryKey());

		DataSet ds = dc.query().from("test_table").select("id").and("birthdate").execute();
		assertTrue(ds.next());
		assertEquals("Row[values=[1, null]]", ds.getRow().toString());
		assertEquals("java.lang.Integer", ds.getRow().getValue(0).getClass().getName());
		assertTrue(ds.next());
		assertEquals("Row[values=[2, 2011-12-21]]", ds.getRow().toString());
		assertEquals("java.sql.Date", ds.getRow().getValue(1).getClass().getName());
		assertFalse(ds.next());
		ds.close();

		dc.executeUpdate(new UpdateScript() {
			@Override
			public void run(UpdateCallback callback) {
				callback.dropTable("test_table").execute();
			}
		});
	}
	
	public void testCharOfSizeOne() throws Exception {
		JdbcTestTemplates.meaningOfOneSizeChar(_connection);
	}

	public void testAlternativeConnectionString() throws Exception {
		_connection = DriverManager.getConnection("jdbc:mysql://localhost", USERNAME, PASSWORD);
		DataContext dc = new JdbcDataContext(_connection, TableType.DEFAULT_TABLE_TYPES, "sakila");
		Schema[] schemas = dc.getSchemas();
		assertEquals("[Schema[name=mysql], Schema[name=performance_schema], Schema[name=portal], "
				+ "Schema[name=sakila], Schema[name=world]]", Arrays.toString(schemas));

		Table table = dc.getSchemaByName("sakila").getTableByName("film");
		Query q = new Query().from(table).select(table.getColumns());
		DataSet data = dc.executeQuery(q);
		TableModel tableModel = new DataSetTableModel(data);
		assertEquals(13, tableModel.getColumnCount());
		assertEquals(1000, tableModel.getRowCount());
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		_connection.close();
	}

	public void testGetCatalogNames() throws Exception {
		JdbcDataContext strategy = new JdbcDataContext(_connection);
		assertTrue(strategy.getQueryRewriter() instanceof MysqlQueryRewriter);
		String[] catalogNames = strategy.getCatalogNames();
		assertEquals("[information_schema, mysql, performance_schema, portal, sakila, world]",
				Arrays.toString(catalogNames));
	}

	public void testGetDefaultSchema() throws Exception {
		DataContext dc = new JdbcDataContext(_connection);
		Schema schema = dc.getDefaultSchema();
		assertEquals("sakila", schema.getName());
	}

	public void testExecuteQuery() throws Exception {
		DataContext dc = new JdbcDataContext(_connection);
		Schema schema = dc.getDefaultSchema();
		Table actorTable = schema.getTableByName("actor");
		assertEquals(
				"[Column[name=actor_id,columnNumber=0,type=SMALLINT,nullable=false,nativeType=SMALLINT UNSIGNED,columnSize=5], Column[name=first_name,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=45], Column[name=last_name,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=45], Column[name=last_update,columnNumber=3,type=TIMESTAMP,nullable=false,nativeType=TIMESTAMP,columnSize=19]]",
				Arrays.toString(actorTable.getColumns()));
		Table filmTable = schema.getTableByName("film");
		assertEquals(
				"[Column[name=film_id,columnNumber=0,type=SMALLINT,nullable=false,nativeType=SMALLINT UNSIGNED,columnSize=5], Column[name=title,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=255], Column[name=description,columnNumber=2,type=LONGVARCHAR,nullable=true,nativeType=TEXT,columnSize=65535], Column[name=release_year,columnNumber=3,type=DATE,nullable=true,nativeType=YEAR,columnSize=0], Column[name=language_id,columnNumber=4,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3], Column[name=original_language_id,columnNumber=5,type=TINYINT,nullable=true,nativeType=TINYINT UNSIGNED,columnSize=3], Column[name=rental_duration,columnNumber=6,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3], Column[name=rental_rate,columnNumber=7,type=DECIMAL,nullable=false,nativeType=DECIMAL,columnSize=4], Column[name=length,columnNumber=8,type=SMALLINT,nullable=true,nativeType=SMALLINT UNSIGNED,columnSize=5], Column[name=replacement_cost,columnNumber=9,type=DECIMAL,nullable=false,nativeType=DECIMAL,columnSize=5], Column[name=rating,columnNumber=10,type=CHAR,nullable=true,nativeType=ENUM,columnSize=5], Column[name=special_features,columnNumber=11,type=CHAR,nullable=true,nativeType=SET,columnSize=54], Column[name=last_update,columnNumber=12,type=TIMESTAMP,nullable=false,nativeType=TIMESTAMP,columnSize=19]]",
				Arrays.toString(filmTable.getColumns()));
		Table filmActorJoinTable = schema.getTableByName("film_actor");
		assertEquals(
				"[Column[name=actor_id,columnNumber=0,type=SMALLINT,nullable=false,nativeType=SMALLINT UNSIGNED,columnSize=5], "
						+ "Column[name=film_id,columnNumber=1,type=SMALLINT,nullable=false,nativeType=SMALLINT UNSIGNED,columnSize=5], "
						+ "Column[name=last_update,columnNumber=2,type=TIMESTAMP,nullable=false,nativeType=TIMESTAMP,columnSize=19]]",
				Arrays.toString(filmActorJoinTable.getColumns()));

		Query q = new Query();
		q.from(new FromItem(actorTable).setAlias("a"));
		q.select(actorTable.getColumns());
		q.getSelectClause().getItem(0).setAlias("foo-bar");
		assertEquals(
				"SELECT a.`actor_id` AS foo-bar, a.`first_name`, a.`last_name`, a.`last_update` FROM sakila.`actor` a",
				q.toString());
		FilterItem f1 = new FilterItem(q.getSelectClause().getItem(0), OperatorType.EQUALS_TO, 5);
		FilterItem f2 = new FilterItem(q.getSelectClause().getItem(0), OperatorType.EQUALS_TO, 8);
		q.where(new FilterItem(f1, f2));

		DataSet dataSet = dc.executeQuery(q);
		TableModel tableModel = new DataSetTableModel(dataSet);
		assertEquals(4, tableModel.getColumnCount());
		assertEquals(2, tableModel.getRowCount());
		assertEquals("LOLLOBRIGIDA", tableModel.getValueAt(0, 2));

		q.setMaxRows(1);
		dataSet = dc.executeQuery(q);
		tableModel = new DataSetTableModel(dataSet);
		assertEquals(4, tableModel.getColumnCount());
		assertEquals(1, tableModel.getRowCount());
		assertEquals("LOLLOBRIGIDA", tableModel.getValueAt(0, 2));
		
		q.setMaxRows(1);
		q.setFirstRow(2);
        dataSet = dc.executeQuery(q);
        tableModel = new DataSetTableModel(dataSet);
        assertEquals(4, tableModel.getColumnCount());
        assertEquals(1, tableModel.getRowCount());
        assertEquals("JOHANSSON", tableModel.getValueAt(0, 2));

		q.getWhereClause().removeItems();
		q.setMaxRows(25);
		q.setFirstRow(1);
		dataSet = dc.executeQuery(q);
		tableModel = new DataSetTableModel(dataSet);
		assertEquals(4, tableModel.getColumnCount());
		assertEquals(25, tableModel.getRowCount());
		assertEquals("GUINESS", tableModel.getValueAt(0, 2).toString());
	}

	// Test to query the film table (caused troubles in DataCleaner)
	public void testFilmQuery() throws Exception {
		DataContext dc = new JdbcDataContext(_connection);
		Table table = dc.getDefaultSchema().getTableByName("film");
		Query q = new Query().select(table.getColumns()).from(table).setMaxRows(400);
		dc.executeQuery(q);
	}

	public void testGetSchema() throws Exception {
		DataContext dc = new JdbcDataContext(_connection);
		Schema[] schemas = dc.getSchemas();
		assertEquals(5, schemas.length);
		Schema schema = dc.getDefaultSchema();

		assertEquals("[Table[name=actor,type=TABLE,remarks=], " + "Table[name=address,type=TABLE,remarks=], "
				+ "Table[name=category,type=TABLE,remarks=], " + "Table[name=city,type=TABLE,remarks=], "
				+ "Table[name=country,type=TABLE,remarks=], " + "Table[name=customer,type=TABLE,remarks=], "
				+ "Table[name=film,type=TABLE,remarks=], " + "Table[name=film_actor,type=TABLE,remarks=], "
				+ "Table[name=film_category,type=TABLE,remarks=], " + "Table[name=film_text,type=TABLE,remarks=], "
				+ "Table[name=inventory,type=TABLE,remarks=], " + "Table[name=language,type=TABLE,remarks=], "
				+ "Table[name=payment,type=TABLE,remarks=], " + "Table[name=rental,type=TABLE,remarks=], "
				+ "Table[name=staff,type=TABLE,remarks=], " + "Table[name=store,type=TABLE,remarks=], "
				+ "Table[name=actor_info,type=VIEW,remarks=], " + "Table[name=customer_list,type=VIEW,remarks=], "
				+ "Table[name=film_list,type=VIEW,remarks=], "
				+ "Table[name=nicer_but_slower_film_list,type=VIEW,remarks=], "
				+ "Table[name=sales_by_film_category,type=VIEW,remarks=], "
				+ "Table[name=sales_by_store,type=VIEW,remarks=], " + "Table[name=staff_list,type=VIEW,remarks=]]",
				Arrays.toString(schema.getTables()));

		Table filmTable = schema.getTableByName("film");
		assertEquals(
				"[Column[name=film_id,columnNumber=0,type=SMALLINT,nullable=false,nativeType=SMALLINT UNSIGNED,columnSize=5], "
						+ "Column[name=title,columnNumber=1,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=255], "
						+ "Column[name=description,columnNumber=2,type=LONGVARCHAR,nullable=true,nativeType=TEXT,columnSize=65535], "
						+ "Column[name=release_year,columnNumber=3,type=DATE,nullable=true,nativeType=YEAR,columnSize=0], "
						+ "Column[name=language_id,columnNumber=4,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3], "
						+ "Column[name=original_language_id,columnNumber=5,type=TINYINT,nullable=true,nativeType=TINYINT UNSIGNED,columnSize=3], "
						+ "Column[name=rental_duration,columnNumber=6,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3], "
						+ "Column[name=rental_rate,columnNumber=7,type=DECIMAL,nullable=false,nativeType=DECIMAL,columnSize=4], "
						+ "Column[name=length,columnNumber=8,type=SMALLINT,nullable=true,nativeType=SMALLINT UNSIGNED,columnSize=5], "
						+ "Column[name=replacement_cost,columnNumber=9,type=DECIMAL,nullable=false,nativeType=DECIMAL,columnSize=5], "
						+ "Column[name=rating,columnNumber=10,type=CHAR,nullable=true,nativeType=ENUM,columnSize=5], "
						+ "Column[name=special_features,columnNumber=11,type=CHAR,nullable=true,nativeType=SET,columnSize=54], "
						+ "Column[name=last_update,columnNumber=12,type=TIMESTAMP,nullable=false,nativeType=TIMESTAMP,columnSize=19]]",
				Arrays.toString(filmTable.getColumns()));
		assertEquals(
				"[Relationship[primaryTable=language,primaryColumns=[language_id],foreignTable=film,foreignColumns=[language_id]], Relationship[primaryTable=language,primaryColumns=[language_id],foreignTable=film,foreignColumns=[original_language_id]], Relationship[primaryTable=film,primaryColumns=[film_id],foreignTable=film_actor,foreignColumns=[film_id]], Relationship[primaryTable=film,primaryColumns=[film_id],foreignTable=film_category,foreignColumns=[film_id]], Relationship[primaryTable=film,primaryColumns=[film_id],foreignTable=inventory,foreignColumns=[film_id]]]",
				Arrays.toString(filmTable.getRelationships()));

		dc = new JdbcDataContext(_connection, TableType.DEFAULT_TABLE_TYPES, "sakila");
		schemas = dc.getSchemas();
		assertEquals(6, schemas.length);
		assertEquals("[Table[name=actor,type=TABLE,remarks=], " + "Table[name=address,type=TABLE,remarks=], "
				+ "Table[name=category,type=TABLE,remarks=], " + "Table[name=city,type=TABLE,remarks=], "
				+ "Table[name=country,type=TABLE,remarks=], " + "Table[name=customer,type=TABLE,remarks=], "
				+ "Table[name=film,type=TABLE,remarks=], " + "Table[name=film_actor,type=TABLE,remarks=], "
				+ "Table[name=film_category,type=TABLE,remarks=], " + "Table[name=film_text,type=TABLE,remarks=], "
				+ "Table[name=inventory,type=TABLE,remarks=], " + "Table[name=language,type=TABLE,remarks=], "
				+ "Table[name=payment,type=TABLE,remarks=], " + "Table[name=rental,type=TABLE,remarks=], "
				+ "Table[name=staff,type=TABLE,remarks=], " + "Table[name=store,type=TABLE,remarks=], "
				+ "Table[name=actor_info,type=VIEW,remarks=], " + "Table[name=customer_list,type=VIEW,remarks=], "
				+ "Table[name=film_list,type=VIEW,remarks=], "
				+ "Table[name=nicer_but_slower_film_list,type=VIEW,remarks=], "
				+ "Table[name=sales_by_film_category,type=VIEW,remarks=], "
				+ "Table[name=sales_by_store,type=VIEW,remarks=], " + "Table[name=staff_list,type=VIEW,remarks=]]",
				Arrays.toString(schema.getTables()));

		Table staffView = schema.getTableByName("staff_list");
		assertEquals(
				"[Column[name=ID,columnNumber=0,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3], "
						+ "Column[name=name,columnNumber=1,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=91], "
						+ "Column[name=address,columnNumber=2,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
						+ "Column[name=zip code,columnNumber=3,type=VARCHAR,nullable=true,nativeType=VARCHAR,columnSize=10], "
						+ "Column[name=phone,columnNumber=4,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=20], "
						+ "Column[name=city,columnNumber=5,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
						+ "Column[name=country,columnNumber=6,type=VARCHAR,nullable=false,nativeType=VARCHAR,columnSize=50], "
						+ "Column[name=SID,columnNumber=7,type=TINYINT,nullable=false,nativeType=TINYINT UNSIGNED,columnSize=3]]",
				Arrays.toString(staffView.getColumns()));
	}

	public void testSplitQuery() throws Exception {
		DataContext dc = new JdbcDataContext(_connection, TableType.DEFAULT_TABLE_TYPES, "sakila");
		Schema schema = dc.getSchemaByName("sakila");
		Table staffListTable = schema.getTableByName("staff_list");
		assertNotNull(staffListTable);
		Table paymentTable = schema.getTableByName("payment");
		assertNotNull(paymentTable);
		Column countryColumn = staffListTable.getColumnByName("country");
		assertNotNull(countryColumn);
		Column paymentColumn = paymentTable.getColumns()[0];
		assertNotNull(paymentColumn);
		Query q = new Query().from(staffListTable, "sl").from(paymentTable, "e").select(countryColumn, paymentColumn);
		assertEquals("SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e",
				q.toString());

		QuerySplitter qs = new QuerySplitter(dc, q);
		assertEquals(32098, qs.getRowCount());
		List<Query> splitQueries = qs.setMaxRows(8000).splitQuery();
		assertEquals(7, splitQueries.size());
		assertEquals(
				"[SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` < 4013 OR e.`rental_id` IS NULL) AND (e.`customer_id` < 300 OR e.`customer_id` IS NULL), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` < 4013 OR e.`rental_id` IS NULL) AND (e.`customer_id` > 300 OR e.`customer_id` = 300), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` > 4013 OR e.`rental_id` = 4013) AND (e.`rental_id` < 8025 OR e.`rental_id` = 4013) AND (e.`payment_id` < 8025 OR e.`payment_id` IS NULL), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` > 4013 OR e.`rental_id` = 4013) AND (e.`rental_id` < 8025 OR e.`rental_id` = 4013) AND (e.`payment_id` > 8025 OR e.`payment_id` = 8025), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` > 8025 OR e.`rental_id` = 8025) AND (e.`rental_id` < 12037 OR e.`rental_id` = 8025) AND (e.`amount` < 6 OR e.`amount` IS NULL), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` > 8025 OR e.`rental_id` = 8025) AND (e.`rental_id` < 12037 OR e.`rental_id` = 8025) AND (e.`amount` > 6 OR e.`amount` = 6), SELECT sl.`country`, e.`payment_id` FROM sakila.`staff_list` sl, sakila.`payment` e WHERE (e.`rental_id` > 12037 OR e.`rental_id` = 12037)]",
				Arrays.toString(splitQueries.toArray()));

		DataSet data = qs.executeQueries();
		int count = 0;
		while (data.next()) {
			count++;
		}
		data.close();
		assertEquals(32098, count);
	}

	public void testQueryWithSingleQuote() throws Exception {
		DataContext dc = new JdbcDataContext(_connection, TableType.DEFAULT_TABLE_TYPES, "sakila");
		Query q = dc.query().from("category").selectCount().where("name").eq("kasper's horror movies").toQuery();
		DataSet ds = dc.executeQuery(q);
		assertTrue(ds.next());
		assertEquals(0, ((Number) ds.getRow().getValue(0)).intValue());
		assertFalse(ds.next());
	}

	public void testWhiteSpaceColumns() throws Exception {
		DatabaseMetaData metaData = _connection.getMetaData();
		assertEquals("`", metaData.getIdentifierQuoteString());
	}
}