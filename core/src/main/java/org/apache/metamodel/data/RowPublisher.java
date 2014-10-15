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
package org.apache.metamodel.data;

/**
 * An object on which a push-style data reader can publish records to a
 * {@link RowPublisherDataSet}. The {@link RowPublisher} acts as a buffer
 * between the publishing and consuming part of a dataset scenario. It will
 * manage a queue of rows and will block calls if the queue is not being
 * read/emptied as fast as it is being filled.
 */
public interface RowPublisher {

	/**
	 * Publishes a row
	 * 
	 * @param row
	 *            the {@link Row} to publish.
	 * @return a boolean indicating whether or not the consumer is still
	 *         interested in more rows.
	 */
	public boolean publish(Row row);

	/**
	 * Publishes a row, represented by an array of values.
	 * 
	 * @param values
	 *            the objects to convert to a row.
	 * @return a boolean indicating whether or not the consumer is still
	 *         interested in more rows.
	 */
	public boolean publish(Object[] values);

	/**
	 * Publishes a row, represented by an array of values and an array of
	 * styles.
	 * 
	 * @param values
	 *            the objects to convert to a row.
	 * @param styles
	 *            the styles that correspond to the values.
	 * @return a boolean indicating whether or not the consumer is still
	 *         interested in more rows.
	 */
	public boolean publish(Object[] values, Style[] styles);

	/**
	 * Invoked to indicate to the consumer that no more rows will be published.
	 */
	public void finished();

}
