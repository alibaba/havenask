package com.taobao.search.iquan.client.common.json.table;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.common.JsonIndexField;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class JsonSubTable {
    private static final Logger logger = LoggerFactory.getLogger(JsonSubTable.class);

    @JsonProperty(value = "sub_table_name")
    private String subTableName;

    @JsonProperty(value = "sub_fields")
    private List<JsonIndexField> subFields;

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(subTableName == null, "subTableName is null");
            ExceptionUtils.throwIfTrue(subFields == null, "subFields is null");
            ExceptionUtils.throwIfTrue(subTableName.isEmpty(),
                    "subTableName is empty");
            ExceptionUtils.throwIfTrue(subFields.stream().anyMatch(v -> !v.isValid()),
                    "one of sub fields is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
