package org.metamodel.jest.elasticsearch;

import io.searchbox.action.GenericResultAbstractAction;

public class JestDeleteScroll extends GenericResultAbstractAction {
    static final int MAX_SCROLL_ID_LENGTH = 1900;

    protected JestDeleteScroll(Builder builder) {
        super(builder);
        this.payload = builder.getScrollId();
        setURI(buildURI());
    }

    @Override
    public String getRestMethodName() {
        return "DELETE";
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_search/scroll";
    }

    public static class Builder extends GenericResultAbstractAction.Builder<JestDeleteScroll, Builder> {
        private final String scrollId;

        public Builder(String scrollId) {
            this.scrollId = scrollId;
        }

        @Override
        public JestDeleteScroll build() {
            return new JestDeleteScroll(this);
        }

        public String getScrollId() {
            return scrollId;
        }
    }

}
