package org.metamodel.jest.elasticsearch;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.apache.metamodel.MetaModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JestClientExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchCreateTableBuilder.class);

    static <T extends JestResult> T execute(JestClient jestClient, Action<T> clientRequest) {
        return execute(jestClient, clientRequest, true);
    }

    static <T extends JestResult> T execute(JestClient jestClient, Action<T> clientRequest, boolean doThrow) {
        try {
            final T result = jestClient.execute(clientRequest);
            logger.debug("{} response: acknowledged={}", clientRequest, result.isSucceeded());
            return result;
        } catch (IOException e) {
            logger.warn("Could not execute command {} ", clientRequest, e);
            if (doThrow) {
                throw new MetaModelException("Could not execute command " + clientRequest, e);
            }
        }

        return null;
    }
}
