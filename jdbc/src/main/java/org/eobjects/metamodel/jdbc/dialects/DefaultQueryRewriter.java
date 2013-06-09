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