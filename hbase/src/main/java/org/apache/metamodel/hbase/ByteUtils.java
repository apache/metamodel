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
package org.apache.metamodel.hbase;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metamodel.MetaModelException;

/**
 * Util class for Converting Object values to Bytes
 * 
 */
public class ByteUtils {

	public static byte[] toBytes(Object o) {
		if (o != null) {
			return toBytes(o, o.getClass());
		}
		return null;
	}

	public static byte[] toBytes(Object value, Class<?> klass) {
		if (klass.isAssignableFrom(String.class)) {
			return Bytes.toBytes(value.toString());
		} else if (klass.equals(int.class)
				|| klass.isAssignableFrom(Integer.class)) {
			return Bytes.toBytes(value instanceof Integer ? (Integer) value
					: new Integer(value.toString()));
		} else if (klass.equals(long.class)
				|| klass.isAssignableFrom(Long.class)) {
			return Bytes.toBytes(value instanceof Long ? (Long) value
					: new Long(value.toString()));
		} else if (klass.equals(boolean.class)
				|| klass.isAssignableFrom(Boolean.class)) {
			return Bytes.toBytes(value instanceof Boolean ? (Boolean) value
					: new Boolean(value.toString()));
		} else if (klass.equals(double.class)
				|| klass.isAssignableFrom(Double.class)) {
			return Bytes.toBytes(value instanceof Double ? (Double) value
					: new Double(value.toString()));
		} else if (klass.equals(float.class)
				|| klass.isAssignableFrom(Float.class)) {
			return Bytes.toBytes(value instanceof Float ? (Float) value
					: new Float(value.toString()));
		} else if (klass.equals(short.class)
				|| klass.isAssignableFrom(Short.class)) {
			return Bytes.toBytes(value instanceof Short ? (Short) value
					: new Short(value.toString()));
		} else if (klass.equals(BigDecimal.class)) {
			return Bytes
					.toBytes(value instanceof BigDecimal ? (BigDecimal) value
							: new BigDecimal(value.toString()));
		} else {
			throw new MetaModelException(
					"Could not find a suitable Type to assign value for give type "
							+ klass.getName());
		}
	}

	/**
	 * Builds Object from Bytes Picked from Hbase.
	 * 
	 * @param b
	 * @param klass
	 * @return
	 */
	public static Object toObject(byte[] b, Class<?> klass) {

		if (klass.isAssignableFrom(String.class)) {
			return Bytes.toString(b);
		} else if (klass.equals(int.class)
				|| klass.isAssignableFrom(Integer.class)) {
			return Bytes.toInt(b);
		} else if (klass.equals(long.class)
				|| klass.isAssignableFrom(Long.class)) {
			return Bytes.toLong(b);
		} else if (klass.equals(boolean.class)
				|| klass.isAssignableFrom(Boolean.class)) {
			return Bytes.toBoolean(b);
		} else if (klass.equals(double.class)
				|| klass.isAssignableFrom(Double.class)) {
			return Bytes.toDouble(b);
		} else if (klass.equals(float.class)
				|| klass.isAssignableFrom(Float.class)) {
			return Bytes.toFloat(b);
		} else if (klass.equals(short.class)
				|| klass.isAssignableFrom(Short.class)) {
			return Bytes.toShort(b);
		} else if (klass.equals(BigDecimal.class)) {
			return Bytes.toBigDecimal(b);
		} else {
			throw new MetaModelException("Could Not find a suitable Type for "
					+ klass.getName());
		}
	}

}
