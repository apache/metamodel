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
package org.eobjects.metamodel.xml;

import java.io.File;
import java.net.URL;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * DataContext implementation for XML files.
 * 
 * @author Kasper Sørensen
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
