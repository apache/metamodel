package org.apache.metamodel.neo4j;

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.data.Document;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.InferentialSchemaBuilder;

public class Neo4jInferentialSchemaBuilder extends InferentialSchemaBuilder {

    public Neo4jInferentialSchemaBuilder() {
        super(Neo4jDataContext.SCHEMA_NAME);
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String determineTable(Document document) {
        final String sourceCollectionName = document.getSourceCollectionName();
        return sourceCollectionName;
    }

}
