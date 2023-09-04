package com.taobao.search.iquan.client.common.json.common;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonIndex {
    private static final Logger logger = LoggerFactory.getLogger(JsonIndex.class);

    @JsonProperty("index_type")
    private String indexType = ConstantDefine.EMPTY;

    @JsonProperty("index_name")
    private String indexName = ConstantDefine.EMPTY;

    @JsonProperty("index_fields")
    private List<String> indexFields = new ArrayList<>();

    @JsonProperty("extend_infos")
    private Map<String, String> extendInfos = new TreeMap<>();

    public JsonIndex() {
    }

    public JsonIndex(String indexType, String indexName, List<String> indexFields, Map<String, String> extendInfos) {
        this.indexType = indexType;
        this.indexName = indexName;
        this.indexFields = indexFields;
        this.extendInfos = extendInfos;
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

    public List<String> getIndexFields() {
        return this.indexFields;
    }

    public void setIndexFields(List<String> fields) {
        this.indexFields = fields;
    }

    public Map<String, String> getExtendInfos() {
        return this.extendInfos;
    }

    public void setExtendInfos(Map<String, String> extendInfos) {
        this.extendInfos = extendInfos;
    }

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(indexType == null || indexType.isEmpty(), "indexType is null or empty");
            ExceptionUtils.throwIfTrue(indexName == null || indexName.isEmpty(), "indexName is null or empty");
            ExceptionUtils.throwIfTrue(indexFields == null || indexFields.isEmpty(), "indexFields is null or empty");
            ExceptionUtils.throwIfTrue(extendInfos == null, "extendInfos is null");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}