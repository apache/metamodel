package org.apache.metamodel.data;

import java.util.Map;

import org.apache.metamodel.convert.DocumentConverter;

/**
 * Represents a document, ie. an object to be turned into a {@link Row} using a
 * {@link DocumentConverter} and to be sourced by a {@link DocumentSource}.
 * 
 * A document does not require any schema. A document will hold key/value pairs
 * where keys are always strings, but values may be arbitrary values.
 */
public class Document {

    private final Map<String, ?> _values;
    private final Object _sourceObject;
    private final String _sourceCollectionName;

    public Document(Map<String, ?> values, Object sourceObject) {
        this(null, values, sourceObject);
    }

    public Document(String sourceCollectionName, Map<String, ?> values, Object sourceObject) {
        _sourceCollectionName = sourceCollectionName;
        _values = values;
        _sourceObject = sourceObject;
    }

    /**
     * Gets the values of the document.
     * 
     * @return
     */
    public Map<String, ?> getValues() {
        return _values;
    }

    /**
     * Gets the source representation of the document, if any.
     * 
     * @return
     */
    public Object getSourceObject() {
        return _sourceObject;
    }

    /**
     * Gets the collection/table name as defined in the source, or a hint about
     * a table name of this document. This method may return null if the
     * {@link DocumentSource} does not have any knowledge about the originating
     * collection name, or if there is no logical way to determine such a name.
     * 
     * @return
     */
    public String getSourceCollectionName() {
        return _sourceCollectionName;
    }
}
