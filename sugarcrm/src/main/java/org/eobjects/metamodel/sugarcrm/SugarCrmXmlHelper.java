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
package org.eobjects.metamodel.sugarcrm;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.eobjects.metamodel.schema.Column;
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
    
    public static SelectFields createSelectFields(Column ... columns) {
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
