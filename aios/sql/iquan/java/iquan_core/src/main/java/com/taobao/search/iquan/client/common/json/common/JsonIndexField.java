package com.taobao.search.iquan.client.common.json.common;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonIndexField extends JsonField {
    private static final Logger logger = LoggerFactory.getLogger(JsonIndexField.class);

    @JsonProperty("index_type")
    private String indexType = ConstantDefine.EMPTY;

    @JsonProperty("index_name")
    private String indexName = ConstantDefine.EMPTY;

    public JsonIndexField() {
    }

    public JsonIndexField(JsonType type) {
        super(type);
    }

    public JsonIndexField(String name, JsonType type) {
        super(name, type);
    }

    public JsonIndexField(String name, JsonType type, boolean isAttribute) {
        super(name, type, isAttribute);
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @JsonIgnore
    public boolean isValid() {
        if (!super.isValid()) {
            return false;
        }

        boolean indexTypeValid = indexType != null && !indexType.isEmpty();
        boolean indexNameValid = indexName != null && !indexName.isEmpty();
        if (indexTypeValid != indexNameValid) {
            logger.error("index type is not valid");
            return false;
        }
        return true;
    }
}