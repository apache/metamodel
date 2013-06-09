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

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.util.Action;
import org.eobjects.metamodel.util.SharedExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract {@link DataSet} implementation for use in scenarios where a
 * pull-oriented style of reading data is not supported. This implementation
 * instead allows a publshing action to publish rows to the dataset in a
 * blocking manner, and thereby to adapt without having to load all rows into
 * memory.
 * 
 * @author Kasper Sørensen
 */
public final class RowPublisherDataSet extends AbstractDataSet {

	private static final Logger logger = LoggerFactory
			.getLogger(RowPublisherDataSet.class);

	private final int _maxRows;
	private final Action<RowPublisher> _publishAction;
	private RowPublisherImpl _rowPublisher;
	private boolean _closed;

	public RowPublisherDataSet(SelectItem[] selectItems, int maxRows,
			Action<RowPublisher> publishAction) {
	    super(selectItems);
		_maxRows = maxRows;
		_publishAction = publishAction;
		_closed = false;
	}

	public int getMaxRows() {
		return _maxRows;
	}

	@Override
	public void close() {
		super.close();
		_closed = true;
		_rowPublisher.finished();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (!_closed) {
			logger.warn(
					"finalize() invoked, but DataSet is not closed. Invoking close() on {}",
					this);
			close();
		}
	}

	@Override
	public boolean next() {
		if (_rowPublisher == null) {
			// first time, create the publisher
			_rowPublisher = new RowPublisherImpl(this);
			logger.info("Starting separate thread for publishing action: {}",
					_publishAction);
			Runnable runnable = new Runnable() {
				public void run() {
					boolean successful = false;
					try {
						_publishAction.run(_rowPublisher);
						logger.debug("Publshing action finished!");
						successful = true;
					} catch (Exception e) {
						_rowPublisher.failed(e);
					}
					if (successful) {
						_rowPublisher.finished();
					}
				};
			};
			SharedExecutorService.get().submit(runnable);
		}
		return _rowPublisher.next();
	}

	@Override
	public Row getRow() {
		if (_rowPublisher == null) {
			return null;
		}
		return _rowPublisher.getRow();
	}

}
