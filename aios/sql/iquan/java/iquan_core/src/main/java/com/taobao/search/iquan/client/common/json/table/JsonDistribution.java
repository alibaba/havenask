package com.taobao.search.iquan.client.common.json.table;

import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.api.schema.HashType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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


    public int getPartitionCnt() {
        return partitionCnt;
    }

    public void setPartitionCnt(int partitionCnt) {
        this.partitionCnt = partitionCnt;
    }

    public List<String> getHashFields() {
        return hashFields;
    }

    public void setHashFields(List<String> hashFields) {
        this.hashFields = hashFields;
    }

    public String getHashFunction() {
        return hashFunction;
    }

    public void setHashFunction(String hashFunction) {
        this.hashFunction = hashFunction;
    }

    public Map<String, String> getHashParams() {
        return hashParams;
    }

    public void setHashParams(Map<String, String> hashParams) {
        this.hashParams = hashParams;
    }

    public JsonDistribution() {
    }

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