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
package org.apache.metamodel.jdbc.dialects;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.AggregateFunction;
import org.apache.metamodel.query.AverageAggregateFunction;
import org.apache.metamodel.query.CountAggregateFunction;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.MaxAggregateFunction;
import org.apache.metamodel.query.MinAggregateFunction;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.SumAggregateFunction;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic query rewriter that adds syntax enhancements that are only possible to resolve just before execution time.
 */
public class DefaultQueryRewriter extends AbstractQueryRewriter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultQueryRewriter.class);

    private static final String SPECIAL_ALIAS_CHARACTERS = "- ,.|*%()!#¤/\\=?;:~";
    private static final Set<Class<? extends FunctionType>> SUPPORTED_FUNCTION_CLASSES = new HashSet<>(
            Arrays.<Class<? extends FunctionType>> asList(CountAggregateFunction.class, SumAggregateFunction.class,
                    MaxAggregateFunction.class, MinAggregateFunction.class, AverageAggregateFunction.class));

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

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize) {
        if (columnType == ColumnType.STRING) {
            // convert STRING to VARCHAR as the default SQL type for strings
            return rewriteColumnType(ColumnType.VARCHAR, columnSize);
        }
        if (columnType == ColumnType.NUMBER) {
            // convert NUMBER to FLOAT as the default SQL type for numbers
            return rewriteColumnType(ColumnType.FLOAT, columnSize);
        }
        return super.rewriteColumnType(columnType, columnSize);
    }

    protected boolean needsQuoting(String alias, String identifierQuoteString) {
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
            } else if (operand instanceof Timestamp) {
                final String timestampLiteral = rewriteTimestamp((Timestamp) operand);
                return rewriteFilterItemWithOperandLiteral(item, timestampLiteral);
            } else if (operand instanceof Iterable || operand.getClass().isArray()) {
                // operand is a set of values (typically in combination with an
                // IN or NOT IN operator). Each individual element must be escaped.

                assert OperatorType.IN.equals(item.getOperator()) || OperatorType.NOT_IN.equals(item.getOperator());

                @SuppressWarnings("unchecked") final List<Object> elements =
                        (List<Object>) CollectionUtils.toList(operand);

                for (ListIterator<Object> it = elements.listIterator(); it.hasNext();) {
                    Object next = it.next();
                    if (next == null) {
                        logger.warn(
                                "element in IN list is NULL, which isn't supported by SQL. Stripping the element from the list: {}",
                                item);
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

    /**
     * Rewrites a (non-compound) {@link FilterItem} when it's operand has already been rewritten into a SQL literal.
     * 
     * @param item
     * @param operandLiteral
     * @return
     */
    protected String rewriteFilterItemWithOperandLiteral(FilterItem item, String operandLiteral) {
        final OperatorType operator = item.getOperator();
        final SelectItem selectItem = item.getSelectItem();
        final StringBuilder sb = new StringBuilder();
        sb.append(selectItem.getSameQueryAlias(false));
        FilterItem.appendOperator(sb, item.getOperand(), operator);
        sb.append(operandLiteral);
        return sb.toString();
    }

    /**
     * Rewrites a {@link Timestamp} into it's literal representation as known by this SQL dialect.
     * 
     * This default implementation returns the JDBC spec's escape syntax for a timestamp: {ts 'yyyy-mm-dd hh:mm:ss.f . .
     * .'}
     * 
     * @param ts
     * @return
     */
    protected String rewriteTimestamp(Timestamp ts) {
        return "{ts '" + ts.toString() + "'}";
    }

    @Override
    public boolean isScalarFunctionSupported(ScalarFunction function) {
        return SUPPORTED_FUNCTION_CLASSES.contains(function.getClass());
    }

    @Override
    public boolean isAggregateFunctionSupported(AggregateFunction function) {
        return SUPPORTED_FUNCTION_CLASSES.contains(function.getClass());
    }

    @Override
    public boolean isFirstRowSupported(final Query query) {
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