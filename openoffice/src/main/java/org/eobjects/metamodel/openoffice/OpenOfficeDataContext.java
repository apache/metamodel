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

package org.eobjects.metamodel.openoffice;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.eobjects.metamodel.AbstractDataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenOfficeDataContext extends AbstractDataContext {

	private static final Logger logger = LoggerFactory
			.getLogger(OpenOfficeDataContext.class);
	private JdbcDataContext _dataContextDelegate;
	private Connection _connection;

	public OpenOfficeDataContext(File dbFile) throws MetaModelException {
		try {
			String databaseName = dbFile.getName();
			File tempDir = FileHelper.getTempDir();

			ZipFile zipFile = new ZipFile(dbFile);
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				String entryName = entry.getName();
				if (entryName.startsWith("database/")) {
					File destFile = new File(tempDir, databaseName + "."
							+ entryName.substring(9));
					destFile.createNewFile();

					if (logger.isDebugEnabled()) {
						logger.debug("Processing entry: " + entryName);
						logger.debug("Writing temp file: "
								+ destFile.getAbsolutePath());
					}

					byte[] buffer = new byte[1024];
					InputStream inputStream = zipFile.getInputStream(entry);
					OutputStream outputStream = new BufferedOutputStream(
							new FileOutputStream(destFile));
					for (int len = inputStream.read(buffer); len >= 0; len = inputStream
							.read(buffer)) {
						outputStream.write(buffer, 0, len);
					}
					inputStream.close();
					outputStream.close();
					destFile.deleteOnExit();
				} else {
					logger.debug("Ignoring entry: " + entryName);
				}
			}
			zipFile.close();

			Class.forName("org.hsqldb.jdbcDriver");
			String url = "jdbc:hsqldb:file:" + tempDir.getAbsolutePath() + "/"
					+ databaseName;
			logger.info("Using database URL: " + url);
			_connection = DriverManager.getConnection(url, "SA", "");
			_connection.setReadOnly(true);
			_dataContextDelegate = new JdbcDataContext(_connection,
					TableType.DEFAULT_TABLE_TYPES, null);
		} catch (Exception e) {
			throw new MetaModelException(e);
		}
	}

	@Override
	public DataSet executeQuery(Query query) throws MetaModelException {
		return _dataContextDelegate.executeQuery(query);
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		_connection.close();
	}

	public Connection getConnection() {
		return _connection;
	}

	@Override
	protected String[] getSchemaNamesInternal() {
		return _dataContextDelegate.getSchemaNames();
	}

	@Override
	protected String getDefaultSchemaName() {
		return _dataContextDelegate.getDefaultSchemaName();
	}

	@Override
	protected Schema getSchemaByNameInternal(String name) {
		return _dataContextDelegate.getSchemaByName(name);
	}
}