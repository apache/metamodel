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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.metamodel.schema.Table;

/**
 * Defines a table layout for {@link XmlSaxDataContext}. This class is used as
 * an instruction set of xpath expressions for the datacontext to create
 * {@link Table}s.
 * 
 * These types of expressions are allowed in the {@link XmlSaxTableDef}:
 * 
 * <ul>
 * <li>Element selectors: "/path/to/element"</li>
 * <li>Attribute selectors: "/path/to/element@attribute"</li>
 * <li>Element index selectors: "index(/path/to)" (can be used to make
 * cross-references to parent tags)</li>
 * </ul>
 * 
 * If, for example, this is your XML document:
 * 
 * <pre>
 * &lt;root&gt;
 * 	&lt;organization type="company"&gt;
 * 		&lt;name&gt;Company A&lt;/name&gt;
 * 		&lt;employees&gt;
 * 			&lt;employee&gt;
 * 				&lt;name&gt;John Doe&lt;/name&gt;
 * 				&lt;gender&gt;M&lt;/gender&gt;
 * 			&lt;/employee&gt;
 * 			&lt;employee&gt;
 * 				&lt;name&gt;Jane Doe&lt;/name&gt;
 * 				&lt;gender&gt;F&lt;/gender&gt;
 * 			&lt;/employee&gt;
 * 		&lt;/employees&gt;
 * 	&lt;/organization&gt;
 * 	&lt;organization type="government"&gt;
 * 		&lt;name&gt;Company B&lt;/name&gt;
 * 		&lt;employees&gt;
 * 			&lt;employee&gt;
 * 				&lt;name&gt;Susan&lt;/name&gt;
 * 				&lt;gender&gt;F&lt;/gender&gt;
 * 			&lt;/employee&gt;
 * 		&lt;/employees&gt;
 * 	&lt;/organization&gt;
 * &lt;/root&gt;
 * </pre>
 * 
 * Then if you wanted to extract information about organizations, these xpaths
 * could work:
 * <ul>
 * <li>Organization row scope: "/root/organization"</li>
 * <li>Organization name: "/root/organization/name"</li>
 * <li>Organization type: "/root/organization@type"</li>
 * </ul>
 * 
 * Or if you wanted to extract information about employees:
 * <ul>
 * <li>Employee row scope: "/root/organization/employees/employee"</li>
 * <li>Employee name: "/root/organization/employees/employee/name"</li>
 * <li>Employee gender: "/root/organization/employees/employee/gender"</li>
 * <li>Employee organization index: "index(/root/organization)"</li>
 * </ul>
 */
public final class XmlSaxTableDef implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String _rowXpath;
	private final String[] _valueXpaths;

	/**
	 * Constructs a {@link XmlSaxTableDef} based on an xpath expression for the
	 * row scope and an array of xpath expressions for the individual values
	 * (columns) within a row.
	 * 
	 * @param rowXpath
	 *            an xpath expression for the scope of a record, eg.
	 *            /companies/company/employee
	 * @param valueXpaths
	 *            an array of xpath expressions for the individual values
	 *            (columns) of a row. eg: [/companies/company/employee/name,
	 *            /companies/company/employee/gender, index(/companies/company)]
	 */
	public XmlSaxTableDef(String rowXpath, String[] valueXpaths) {
		_rowXpath = rowXpath;
		_valueXpaths = valueXpaths;
	}

	public String getRowXpath() {
		return _rowXpath;
	}

	public String[] getValueXpaths() {
		return _valueXpaths;
	}

	@Override
	public int hashCode() {
		return _rowXpath.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		XmlSaxTableDef other = (XmlSaxTableDef) obj;
		if (_rowXpath == null) {
			if (other._rowXpath != null)
				return false;
		} else if (!_rowXpath.equals(other._rowXpath))
			return false;
		if (!Arrays.equals(_valueXpaths, other._valueXpaths))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "XmlSaxTableDef[rowXpath=" + _rowXpath + ",valueXpaths="
				+ Arrays.toString(_valueXpaths) + "]";
	}
}
