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
package org.apache.metamodel.query;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Assert;
import org.junit.Test;

public class SubstringFunctionTest {

    private final SubstringFunction javaStyleFunction = SubstringFunction.createJavaStyle();
    private final SubstringFunction sqlStyleFunction = SubstringFunction.createSqlStyle();

    @Test
    public void testSubstringVanilla() {
        Assert.assertEquals("2", runTest(javaStyleFunction, "123456", 1, 2));
        Assert.assertEquals("1234", runTest(javaStyleFunction, "123456", 0, 4));
        Assert.assertEquals("34", runTest(javaStyleFunction, "123456", 2, 4));
        
        Assert.assertEquals("1", runTest(sqlStyleFunction, "123456", 1, 1));
        Assert.assertEquals("1234", runTest(sqlStyleFunction, "123456", 1, 4));
        Assert.assertEquals("34", runTest(sqlStyleFunction, "123456", 3, 2));
    }
    
    @Test
    public void testSubstringBadOrWeirdParamValues() {
        Assert.assertEquals("", runTest(javaStyleFunction, "123456", 0, 0));
        Assert.assertEquals("1234", runTest(javaStyleFunction, "123456", -10, 4));
        Assert.assertEquals("", runTest(javaStyleFunction, "123456", 4, -1));
    }

    @Test
    public void testSubstringEndIndexTooLarge() {
        Assert.assertEquals("123456", runTest(javaStyleFunction, "123456", 0, 200));
        Assert.assertEquals("56", runTest(javaStyleFunction, "123456", 4, 8));
        
        Assert.assertEquals("123456", runTest(sqlStyleFunction, "123456", 1, 200));
        Assert.assertEquals("56", runTest(sqlStyleFunction, "123456", 5, 5));
    }

    @Test
    public void testSubstringStartIndexTooLarge() {
        Assert.assertEquals("", runTest(javaStyleFunction, "123456", 200, 2));
    }

    @Test
    public void testSubstringOnlyStartIndex() {
        Assert.assertEquals("123456", runTest(javaStyleFunction, "123456", 0));
        Assert.assertEquals("", runTest(javaStyleFunction, "123456", 10));
        Assert.assertEquals("3456", runTest(javaStyleFunction, "123456", 2));
    }

    private String runTest(ScalarFunction f, String str, Object... params) {
        SelectItem selectItem = new SelectItem(new MutableColumn("column"));
        DataSetHeader header = new SimpleDataSetHeader(new SelectItem[] { selectItem });
        return (String) f.evaluate(new DefaultRow(header, new Object[] { str }), params, selectItem);
    }
}
