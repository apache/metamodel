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
package org.apache.metamodel.sugarcrm;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.metamodel.schema.Column;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import com.sugarcrm.ws.soap.SelectFields;

/**
 * A small toolkit for making DOM navigation easier, since SugarCRM's responses
 * are only sparsely recognized by JAX-WS / JAXB.
 */
final class SugarCrmXmlHelper {
    
    public static SelectFields createSelectFields(List<Column> columns) {
        final SelectFields selectFields = new SelectFields();
        selectFields.setArrayType("xsd:string[]");

        final Document document;
        try {
            document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }

        final List<Object> fieldList = selectFields.getAny();
        for (Column column : columns) {
            final String fieldName = column.getName();
            
            final Element elem = document.createElement("v");
            elem.appendChild(document.createTextNode(fieldName));
            
            fieldList.add(elem);
        }
        return selectFields;
    }

    public static Element getChildElement(Node node, String childName) {
        NodeList childNodes = node.getChildNodes();
        int length = childNodes.getLength();
        for (int i = 0; i < length; i++) {
            Node item = childNodes.item(i);
            if (item instanceof Element && item.getNodeName().equals(childName)) {
                return (Element) item;
            }
        }
        return null;
    }

    public static List<Element> getChildElements(Element element) {
        final List<Element> list = new ArrayList<Element>();
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Element) {
                list.add((Element) node);
            }
        }
        return list;
    }

    public static String getTextContent(Element element) {
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            final Node node = childNodes.item(i);
            if (node instanceof Text) {
                String textContent = ((Text) node).getWholeText();
                textContent = textContent.trim();
                return textContent;
            }
        }
        return null;
    }

    public static String getChildElementText(Node node, String childName) {
        Element child = getChildElement(node, childName);
        if (child != null) {
            return getTextContent(child);
        }
        return null;
    }
}
