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
package org.apache.metamodel;

import org.apache.metamodel.schema.Schema;
import org.ektorp.util.Assert;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class MongoDbDataContextFactoryTest extends MongoDbTestCase {

	private MongoDatabase db;
	private MongoClient client;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		if (isConfigured()) {
			client = new MongoClient(new MongoClientURI("mongodb://" + getHostname() + ":27017"));
			db = client.getDatabase(getDatabaseName());
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		if (isConfigured()) {
			db.drop();
		}
	}

	public void testcreateMongoDbDataContext() {
		char[] pass = new char[0];
		if (getPassWord()!=null){
			pass = getPassWord().toCharArray();
		}
		UpdateableDataContext dc = DataContextFactory.createMongoDbDataContext(getHostname(), 27017, getDatabaseName(),
				getUserName(), pass);
		Assert.notNull(dc);
		Schema schema = dc.getDefaultSchema();
		System.out.println(String.format("Schéma %1$s - %2$s collections", schema.getName(), schema.getTableCount()));
	}
}
