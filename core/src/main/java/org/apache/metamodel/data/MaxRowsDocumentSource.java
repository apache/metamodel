package org.apache.metamodel.data;

/**
 * A {@link DocumentSource} that has a max rows condition on it, that will make
 * it stop serving documents after a certain limit.
 */
public class MaxRowsDocumentSource implements DocumentSource {

    private final DocumentSource _delegate;
    private volatile int _rowsLeft;

    public MaxRowsDocumentSource(DocumentSource delegate, int maxRows) {
        _delegate = delegate;
        _rowsLeft = maxRows;
    }

    @Override
    public Document next() {
        if (_rowsLeft > 0) {
            Document next = _delegate.next();
            _rowsLeft--;
            return next;
        }
        return null;
    }

    @Override
    public void close() {
        _delegate.close();
    }

}
