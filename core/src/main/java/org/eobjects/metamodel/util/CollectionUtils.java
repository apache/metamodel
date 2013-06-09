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
package org.eobjects.metamodel.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Various utility methods for handling of collections and arrays.
 * 
 * @author Kasper Sørensen
 */
public final class CollectionUtils {

	private CollectionUtils() {
		// prevent instantiation
	}

	/**
	 * Concatenates two arrays
	 * 
	 * @param existingArray an existing array
	 * @param elements the elements to add to the end of it
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> E[] array(final E[] existingArray, final E... elements) {
		if (existingArray == null) {
			return elements;
		}
		Object result = Array.newInstance(existingArray.getClass()
				.getComponentType(), existingArray.length + elements.length);
		System.arraycopy(existingArray, 0, result, 0, existingArray.length);
		System.arraycopy(elements, 0, result, existingArray.length,
				elements.length);
		return (E[]) result;
	}

	public static <E> List<E> concat(boolean removeDuplicates,
			Collection<? extends E> firstCollection,
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

	private static <E> void addElements(boolean removeDuplicates,
			final List<E> result, Collection<? extends E> elements) {
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

	private static Object arrayRemoveInternal(Object array,
			Object elementToRemove) {
		boolean found = false;
		final int oldLength = Array.getLength(array);
		if (oldLength == 0) {
			return array;
		}
		final int newLength = oldLength - 1;
		final Object result = Array.newInstance(array.getClass()
				.getComponentType(), newLength);
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

	public static <E> List<E> filter(E[] items, Predicate<? super E> predicate) {
		return filter(Arrays.asList(items), predicate);
	}

	public static <E> List<E> filter(Iterable<E> items,
			Predicate<? super E> predicate) {
		List<E> result = new ArrayList<E>();
		for (E e : items) {
			if (predicate.eval(e).booleanValue()) {
				result.add(e);
			}
		}
		return result;
	}

	public static <I, O> List<O> map(I[] items, Func<? super I, O> func) {
		return map(Arrays.asList(items), func);
	}

	public static <I, O> List<O> map(Iterable<I> items, Func<? super I, O> func) {
		List<O> result = new ArrayList<O>();
		for (I item : items) {
			O output = func.eval(item);
			result.add(output);
		}
		return result;
	}

	public static <E> void forEach(E[] items, Action<? super E> action) {
		forEach(Arrays.asList(items), action);
	}

	public static <E> void forEach(Iterable<E> items, Action<? super E> action) {
		for (E item : items) {
			try {
				action.run(item);
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
