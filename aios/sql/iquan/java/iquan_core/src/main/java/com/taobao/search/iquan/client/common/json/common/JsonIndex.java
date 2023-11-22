package com.taobao.search.iquan.client.common.json.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
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
