package com.taobao.search.iquan.client.common.json.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.schema.HashType;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class JsonDistribution {
    private static final Logger logger = LoggerFactory.getLogger(JsonDistribution.class);

    @JsonProperty(value = "partition_cnt", required = true)
    int partitionCnt = 0;

    @JsonProperty(value = "hash_fields")
    List<String> hashFields = new ArrayList<>();

    @JsonProperty(value = "hash_function", required = true)
    String hashFunction = HashType.Constant.INVALID;

    @JsonProperty(value = "hash_params")
    Map<String, String> hashParams = new TreeMap<>();

    @JsonIgnore
    public boolean isValid() {
        boolean isValid = true;
        try {
            //broadcast and single distribution only check partitionCnt, other check in Distribution.java
            ExceptionUtils.throwIfTrue(partitionCnt <= 0, "distribution partitionCnt must be > 0");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }
}
