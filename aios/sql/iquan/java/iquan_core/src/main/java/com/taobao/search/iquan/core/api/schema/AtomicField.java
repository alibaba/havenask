package com.taobao.search.iquan.core.api.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AtomicField extends AbstractField {
    private static final Logger logger = LoggerFactory.getLogger(AtomicField.class);

    private String indexName = "";
    private IndexType indexType = IndexType.IT_NONE;
    private boolean isPrimaryKey = false;

    protected AtomicField() {
    }

    public AtomicField(String fieldName, FieldType fieldType, boolean isAttribute) {
        super(fieldName, fieldType, isAttribute);
    }

    public AtomicField(String fieldName, String fieldType, boolean isAttribute) {
        this(fieldName, FieldType.from(fieldType), isAttribute);
    }

    public String getIndexName() {
        return indexName;
    }

    private void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    private void setIndexType(IndexType indexType) {
        this.indexType = indexType;
        if (this.indexType.isPrimaryKey()) {
            isPrimaryKey = true;
        }
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @Override
    public boolean isValid() {
        if (!super.isValid()) {
            return false;
        }

        if (!fieldType.isAtomicType()) {
            logger.error("fieldType is not an atomic field, current type is {}", fieldType.getName());
            return false;
        }

        if (!indexName.isEmpty() && !indexType.isValid()) {
            logger.error("indexName is empty or indexType is not valid");
            return false;
        }

        return true;
    }

    @Override
    public String getDigest() {
        if (indexName.isEmpty()) {
            return super.getDigest();
        } else {
            return super.getDigest() + "|" + indexName + "|" + indexType.getName();
        }
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private AtomicField field;

        public Builder() {
            field = new AtomicField();
        }

        public Builder fieldName(String fieldName) {
            field.setFieldName(fieldName);
            return this;
        }

        public Builder fieldType(String fieldType) {
            field.setFieldType(FieldType.from(fieldType));
            return this;
        }

        public Builder defaultValue(Object value) {
            field.setDefaultValue(value);
            return this;
        }

        public Builder isAttribute(boolean isAttribute) {
            field.setIsAttribute(isAttribute);
            return this;
        }

        public Builder extendInfos(Map<String, String> extendInfos) {
            field.setExtendInfos(extendInfos);
            return this;
        }

        public Builder indexName(String indexName) {
            field.setIndexName(indexName);
            return this;
        }

        public Builder indexType(String indexType) {
            field.setIndexType(IndexType.from(indexType));
            return this;
        }

        public AtomicField build() {
            if (!field.isValid()) {
                logger.error("AtomicField build fail: {}", field.toString());
                return null;
            }
            return field;
        }
    }
}
