package org.apache.metamodel.couchdb;

import org.apache.metamodel.data.Document;
import org.apache.metamodel.schema.builder.ColumnNameAsKeysRowConverter;
import org.ektorp.ViewResult.Row;

public class CouchDbDocumentConverter extends ColumnNameAsKeysRowConverter {

    @Override
    protected Object get(Document document, String columnName) {
        if (CouchDbDataContext.FIELD_ID.equals(columnName)) {
            Row row = (Row) document.getSourceObject();
            return row.getId();
        }
        // TODO Auto-generated method stub
        return super.get(document, columnName);
    }
}
