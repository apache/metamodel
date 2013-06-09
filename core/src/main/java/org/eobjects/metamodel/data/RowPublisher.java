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
package org.eobjects.metamodel.data;

/**
 * An object on which a push-style data reader can publish records to a
 * {@link RowPublisherDataSet}. The {@link RowPublisher} acts as a buffer
 * between the publishing and consuming part of a dataset scenario. It will
 * manage a queue of rows and will block calls if the queue is not being
 * read/emptied as fast as it is being filled.
 * 
 * @author Kasper Sørensen
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
