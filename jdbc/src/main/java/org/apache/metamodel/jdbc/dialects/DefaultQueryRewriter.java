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
package org.eobjects.metamodel.jdbc.dialects;

import java.util.List;
import java.util.ListIterator;

import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.util.CollectionUtils;

/**
 * Generic query rewriter that adds syntax enhancements that are only possible
 * to resolve just before execution time.
 */
public class DefaultQueryRewriter extends AbstractQueryRewriter {

    private static final String SPECIAL_ALIAS_CHARACTERS = "- ,.|*%()!#¤/\\=?;:~";

    public DefaultQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    protected Query beforeRewrite(Query query) {
        query = query.clone();

        JdbcDataContext dataContext = getDataContext();
        if (dataContext != null) {
            String identifierQuoteString = dataContext.getIdentifierQuoteString();
            if (identifierQuoteString != null) {
                List<SelectItem> selectItems = query.getSelectClause().getItems();
                for (SelectItem item : selectItems) {
                    String alias = item.getAlias();
                    if (needsQuoting(alias, identifierQuoteString)) {
                        item.setAlias(identifierQuoteString + alias + identifierQuoteString);
                    }
                }
                List<FromItem> fromItems = query.getFromClause().getItems();
                for (FromItem item : fromItems) {
                    String alias = item.getAlias();
                    if (needsQuoting(alias, identifierQuoteString)) {
                        item.setAlias(identifierQuoteString + alias + identifierQuoteString);
                    }
                }
            }
        }
        return query;
    }

    private boolean needsQuoting(String alias, String identifierQuoteString) {
        boolean result = false;
        if (alias != null && identifierQuoteString != null) {
            if (alias.indexOf(identifierQuoteString) == -1) {
                for (int i = 0; i < SPECIAL_ALIAS_CHARACTERS.length(); i++) {
                    char specialCharacter = SPECIAL_ALIAS_CHARACTERS.charAt(i);
                    if (alias.indexOf(specialCharacter) != -1) {
                        result = true;
                        break;
                    }
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("needsQuoting(" + alias + "," + identifierQuoteString + ") = " + result);
        }
        return result;
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        Object operand = item.getOperand();
        if (operand != null) {
            if (operand instanceof String) {
                String str = (String) operand;
                // escape single quotes
                if (str.indexOf('\'') != -1) {
                    str = escapeQuotes(str);
                    FilterItem replacementFilterItem = new FilterItem(item.getSelectItem(), item.getOperator(), str);
                    return super.rewriteFilterItem(replacementFilterItem);
                }
            } else if (operand instanceof Iterable || operand.getClass().isArray()) {
                // operand is a set of values (typically in combination with an
                // IN operator). Each individual element must be escaped.

                assert item.getOperator() == OperatorType.IN;

                @SuppressWarnings("unchecked")
                final List<Object> elements = (List<Object>) CollectionUtils.toList(operand);

                for (ListIterator<Object> it = elements.listIterator(); it.hasNext();) {
                    Object next = it.next();
                    if (next == null) {
                        logger.warn("element in IN list is NULL, which isn't supported by SQL. Stripping the element from the list: {}", item);
                        it.remove();
                    } else if (next instanceof String) {
                        String str = (String) next;
                        if (str.indexOf('\'') != -1) {
                            str = escapeQuotes(str);
                            it.set(str);
                        }
                    }
                }

                FilterItem replacementFilterItem = new FilterItem(item.getSelectItem(), item.getOperator(), elements);
                return super.rewriteFilterItem(replacementFilterItem);
            }
        }
        return super.rewriteFilterItem(item);
    }

    @Override
    public boolean isFirstRowSupported() {
        return false;
    }

    @Override
    public boolean isMaxRowsSupported() {
        return false;
    }

    @Override
    public String escapeQuotes(String item) {
        return item.replaceAll("\\'", "\\'\\'");
    }
}