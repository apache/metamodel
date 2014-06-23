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
package org.apache.metamodel.xml;

import java.io.File;
import java.net.URL;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * DataContext implementation for XML files.
 * 
 * @deprecated use {@link XmlDomDataContext} or {@link XmlSaxDataContext}
 *             instead.
 */
@Deprecated
public final class XmlDataContext extends XmlDomDataContext {

	public XmlDataContext(File file, boolean autoFlattenTables)
			throws IllegalArgumentException {
		super(file, autoFlattenTables);
	}

	public XmlDataContext(File file) {
		super(file);
	}

	public XmlDataContext(InputSource inputSource, String schemaName,
			boolean autoFlattenTables) {
		super(inputSource, schemaName, autoFlattenTables);
	}

	public XmlDataContext(String schemaName, Document document,
			boolean autoFlattenTables) {
		super(schemaName, document, autoFlattenTables);
	}

	public XmlDataContext(URL url, boolean autoFlattenTables)
			throws IllegalArgumentException {
		super(url, autoFlattenTables);
	}
}
