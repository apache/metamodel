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
package org.apache.metamodel.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

public class CollectionUtilsTest extends TestCase {

    public void testArray1() throws Exception {
        String[] result = CollectionUtils.array(new String[] { "foo", "bar" }, "hello", "world");
        assertEquals("[foo, bar, hello, world]", Arrays.toString(result));
    }

    public void testFindWithKeyIncludingDotAndNestedMap() throws Exception {
        final Map<String, Object> nestedMap = new HashMap<String, Object>();
        nestedMap.put("GivenName", "John");
        nestedMap.put("FamilyName", "Doe");
        nestedMap.put("Titulation", "Mr");

        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("Person.Name", nestedMap);

        assertEquals("John", CollectionUtils.find(map, "Person.Name.GivenName").toString());
    }

    public void testFindWithListIndex() throws Exception {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("Interests", Arrays.asList("Football", "Soccer"));

        assertEquals("Football", CollectionUtils.find(map, "Interests[0]"));
        assertEquals("Soccer", CollectionUtils.find(map, "Interests[1]"));
        assertEquals(null, CollectionUtils.find(map, "Interests[2]"));
    }

    public void testFindWithNestedListsAndMaps() throws Exception {
        final Map<String, Object> address1 = new LinkedHashMap<String, Object>();
        address1.put("city", "Stockholm");
        address1.put("country", "Sweden");

        final Map<String, Object> address2 = new LinkedHashMap<String, Object>();
        address2.put("city", "Copenhagen");
        address2.put("country", "Denmark");

        final Object[] addressesMap = new Object[] { address1, address2 };

        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("Person.Addresses", addressesMap);

        assertEquals("Stockholm", CollectionUtils.find(map, "Person.Addresses[0].city").toString());
        assertEquals("{city=Copenhagen, country=Denmark}", CollectionUtils.find(map, "Person.Addresses[1]").toString());
        assertEquals(null, CollectionUtils.find(map, "Person.Addresses[4].country"));
        
        assertEquals(null, CollectionUtils.find(map, "Foo.bar"));
        assertEquals(null, CollectionUtils.find(map, "Person.Addresses.Foo"));
    }

    public void testArray2() throws Exception {
        Object[] existingArray = new Object[] { 'c' };
        Object[] result = CollectionUtils.array(existingArray, "foo", 1, "bar");

        assertEquals("[c, foo, 1, bar]", Arrays.toString(result));
    }

    public void testConcat() throws Exception {
        List<String> list1 = new ArrayList<String>();
        list1.add("hello");
        list1.add("hello");
        list1.add("world");
        List<String> list2 = new ArrayList<String>();
        list2.add("howdy");
        list2.add("world");
        List<String> list3 = new ArrayList<String>();
        list3.add("hi");
        list3.add("world");

        List<String> result = CollectionUtils.concat(true, list1, list2, list3);
        assertEquals("[hello, world, howdy, hi]", result.toString());
        assertEquals(4, result.size());

        result = CollectionUtils.concat(false, list1, list2, list3);
        assertEquals("[hello, hello, world, howdy, world, hi, world]", result.toString());
        assertEquals(7, result.size());
    }

    public void testMap() throws Exception {
        List<String> strings = new ArrayList<String>();
        strings.add("hi");
        strings.add("world");

        List<Integer> ints = CollectionUtils.map(strings, new Func<String, Integer>() {
            @Override
            public Integer eval(String arg) {
                return arg.length();
            }
        });
        assertEquals("[2, 5]", ints.toString());
    }

    public void testFilter() throws Exception {
        List<String> list = new ArrayList<String>();
        list.add("foo");
        list.add("bar");
        list.add("3");
        list.add("2");

        list = CollectionUtils.filter(list, new Predicate<String>() {
            @Override
            public Boolean eval(String arg) {
                return arg.length() > 1;
            }
        });

        assertEquals(2, list.size());
        assertEquals("[foo, bar]", list.toString());
    }

    public void testArrayRemove() throws Exception {
        String[] arr = new String[] { "a", "b", "c", "d", "e" };
        arr = CollectionUtils.arrayRemove(arr, "c");
        assertEquals("[a, b, d, e]", Arrays.toString(arr));

        arr = CollectionUtils.arrayRemove(arr, "e");
        assertEquals("[a, b, d]", Arrays.toString(arr));

        arr = CollectionUtils.arrayRemove(arr, "e");
        assertEquals("[a, b, d]", Arrays.toString(arr));

        arr = CollectionUtils.arrayRemove(arr, "a");
        assertEquals("[b, d]", Arrays.toString(arr));
    }

    public void testToList() throws Exception {
        assertTrue(CollectionUtils.toList(null).isEmpty());
        assertEquals("[foo]", CollectionUtils.toList("foo").toString());
        assertEquals("[foo, bar]", CollectionUtils.toList(new String[] { "foo", "bar" }).toString());

        List<Integer> ints = Arrays.asList(1, 2, 3);
        assertSame(ints, CollectionUtils.toList(ints));

        assertEquals("[1, 2, 3]", CollectionUtils.toList(ints.iterator()).toString());
        assertEquals("[1, 2, 3]", CollectionUtils.toList(new HashSet<Integer>(ints)).toString());
    }
}
