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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Various utility methods for handling of collections and arrays.
 */
public final class CollectionUtils {

    private CollectionUtils() {
        // prevent instantiation
    }

    /**
     * Searches a map for a given key. The key can be a regular map key, or a
     * simple expression of the form:
     * 
     * <ul>
     * <li>foo.bar (will lookup 'foo', and then 'bar' in a potential nested map)
     * </li>
     * <li>foo.bar[0].baz (will lookup 'foo', then 'bar' in a potential nested
     * map, then pick the first element in case it is a list/array and then pick
     * 'baz' from the potential map at that position).
     * </ul>
     * 
     * @param map
     *            the map to search in
     * @param key
     *            the key to resolve
     * @return the object in the map with the given key/expression. Or null if
     *         it does not exist.
     */
    public static Object find(Map<?, ?> map, String key) {
        if (map == null || key == null) {
            return null;
        }
        final Object result = map.get(key);
        if (result == null) {
            return find(map, key, 0);
        }
        return result;
    }

    private static Object find(Map<?, ?> map, String key, int fromIndex) {
        final int indexOfDot = key.indexOf('.', fromIndex);
        final int indexOfBracket = key.indexOf('[', fromIndex);
        int indexOfEndBracket = -1;
        int arrayIndex = -1;

        boolean hasDot = indexOfDot != -1;
        boolean hasBracket = indexOfBracket != -1;

        if (hasBracket) {
            // also check that there is an end-bracket
            indexOfEndBracket = key.indexOf("]", indexOfBracket);
            hasBracket = indexOfEndBracket != -1;
            if (hasBracket) {
                final String indexString = key.substring(indexOfBracket + 1, indexOfEndBracket);
                try {
                    arrayIndex = Integer.parseInt(indexString);
                } catch (NumberFormatException e) {
                    // not a valid array/list index
                    hasBracket = false;
                }
            }
        }

        if (hasDot && hasBracket) {
            if (indexOfDot > indexOfBracket) {
                hasDot = false;
            } else {
                hasBracket = false;
            }
        }

        if (hasDot) {
            final String prefix = key.substring(0, indexOfDot);
            final Object nestedObject = map.get(prefix);
            if (nestedObject == null) {
                return find(map, key, indexOfDot + 1);
            }
            if (nestedObject instanceof Map) {
                final String remainingPart = key.substring(indexOfDot + 1);
                @SuppressWarnings("unchecked")
                final Map<String, ?> nestedMap = (Map<String, ?>) nestedObject;
                return find(nestedMap, remainingPart);
            }
        }

        if (hasBracket) {
            final String prefix = key.substring(0, indexOfBracket);
            final Object nestedObject = map.get(prefix);
            if (nestedObject == null) {
                return find(map, key, indexOfBracket + 1);
            }

            String remainingPart = key.substring(indexOfEndBracket + 1);

            try {
                final Object valueAtIndex;
                if (nestedObject instanceof List) {
                    valueAtIndex = ((List<?>) nestedObject).get(arrayIndex);
                } else if (nestedObject.getClass().isArray()) {
                    valueAtIndex = Array.get(nestedObject, arrayIndex);
                } else {
                    // no way to extract from a non-array and non-list
                    valueAtIndex = null;
                }

                if (valueAtIndex != null) {
                    if (remainingPart.startsWith(".")) {
                        remainingPart = remainingPart.substring(1);
                    }

                    if (remainingPart.isEmpty()) {
                        return valueAtIndex;
                    }

                    if (valueAtIndex instanceof Map) {
                        @SuppressWarnings("unchecked")
                        final Map<String, ?> nestedMap = (Map<String, ?>) valueAtIndex;
                        return find(nestedMap, remainingPart);
                    } else {
                        // not traversing any further. Should we want to add
                        // support for double-sided arrays, we could do it here.
                    }
                }

            } catch (IndexOutOfBoundsException e) {
                return null;
            }
        }

        return null;
    }

    /**
     * Concatenates two arrays
     * 
     * @param existingArray
     *            an existing array
     * @param elements
     *            the elements to add to the end of it
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <E> E[] array(final E[] existingArray, final E... elements) {
        if (existingArray == null) {
            return elements;
        }
        Object result = Array.newInstance(existingArray.getClass().getComponentType(),
                existingArray.length + elements.length);
        System.arraycopy(existingArray, 0, result, 0, existingArray.length);
        System.arraycopy(elements, 0, result, existingArray.length, elements.length);
        return (E[]) result;
    }

    public static <E> List<E> concat(boolean removeDuplicates, Collection<? extends E> firstCollection,
            Collection<?>... collections) {
        final List<E> result;
        if (removeDuplicates) {
            result = new ArrayList<E>();
            addElements(removeDuplicates, result, firstCollection);
        } else {
            result = new ArrayList<E>(firstCollection);
        }
        for (Collection<?> collection : collections) {
            @SuppressWarnings("unchecked")
            Collection<? extends E> elems = (Collection<? extends E>) collection;
            addElements(removeDuplicates, result, elems);
        }
        return result;
    }

    private static <E> void addElements(boolean removeDuplicates, final List<E> result,
            Collection<? extends E> elements) {
        for (E item : elements) {
            if (removeDuplicates) {
                if (!result.contains(item)) {
                    result.add(item);
                }
            } else {
                result.add(item);
            }
        }
    }

    public static <E> E[] arrayRemove(E[] array, E elementToRemove) {
        @SuppressWarnings("unchecked")
        E[] result = (E[]) arrayRemoveInternal(array, elementToRemove);
        return result;
    }

    public static Object arrayRemove(Object array, Object elementToRemove) {
        return arrayRemoveInternal(array, elementToRemove);
    }

    private static Object arrayRemoveInternal(Object array, Object elementToRemove) {
        boolean found = false;
        final int oldLength = Array.getLength(array);
        if (oldLength == 0) {
            return array;
        }
        final int newLength = oldLength - 1;
        final Object result = Array.newInstance(array.getClass().getComponentType(), newLength);
        int nextIndex = 0;
        for (int i = 0; i < oldLength; i++) {
            final Object e = Array.get(array, i);
            if (e.equals(elementToRemove)) {
                found = true;
            } else {
                if (nextIndex == newLength) {
                    break;
                }
                Array.set(result, nextIndex, e);
                nextIndex++;
            }
        }
        if (!found) {
            return array;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <E> E[] arrayOf(Class<E> elementClass, Object arrayOrElement) {
        if (arrayOrElement == null) {
            return null;
        }
        if (arrayOrElement.getClass().isArray()) {
            return (E[]) arrayOrElement;
        }
        Object result = Array.newInstance(elementClass, 1);
        Array.set(result, 0, arrayOrElement);
        return (E[]) result;
    }

    public static <E> List<E> filter(E[] items, java.util.function.Predicate<? super E> predicate) {
        return filter(Arrays.asList(items), predicate);
    }

    public static <E> List<E> filter(Iterable<E> items, java.util.function.Predicate<? super E> predicate) {
        List<E> result = new ArrayList<E>();
        for (E e : items) {
            if (predicate.test(e)) {
                result.add(e);
            }
        }
        return result;
    }

    public static <I, O> List<O> map(I[] items, Function<? super I, O> func) {
        return map(Arrays.asList(items), func);
    }

    public static <I, O> List<O> map(Iterable<I> items, Function<? super I, O> func) {
        List<O> result = new ArrayList<O>();
        for (I item : items) {
            O output = func.apply(item);
            result.add(output);
        }
        return result;
    }

    public static <E> void forEach(E[] items, Consumer<? super E> action) {
        forEach(Arrays.asList(items), action);
    }

    public static <E> void forEach(Iterable<E> items, Consumer<? super E> action) {
        for (E item : items) {
            try {
                action.accept(item);
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new IllegalStateException("Action threw exception", e);
            }
        }
    }

    public static <E> boolean isNullOrEmpty(E[] arr) {
        return arr == null || arr.length == 0;
    }

    public static boolean isNullOrEmpty(Collection<?> col) {
        return col == null || col.isEmpty();
    }

    /**
     * General purpose list converter method. Will convert arrays, collections,
     * iterables etc. into lists.
     * 
     * If the argument is a single object (such as a String or a POJO) it will
     * be wrapped in a single-element list.
     * 
     * Null will be converted to the empty list.
     * 
     * @param obj
     *            any object
     * @return a list representation of the object
     */
    public static List<?> toList(Object obj) {
        final List<Object> result;
        if (obj == null) {
            result = Collections.emptyList();
        } else if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) obj;
            result = list;
        } else if (obj.getClass().isArray()) {
            int length = Array.getLength(obj);
            result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(Array.get(obj, i));
            }
        } else if (obj instanceof Iterable) {
            result = new ArrayList<Object>();
            for (Object item : (Iterable<?>) obj) {
                result.add(item);
            }
        } else if (obj instanceof Iterator) {
            result = new ArrayList<Object>();
            Iterator<?> it = (Iterator<?>) obj;
            while (it.hasNext()) {
                result.add(it.next());
            }
        } else {
            result = new ArrayList<Object>(1);
            result.add(obj);
        }
        return result;
    }
}
