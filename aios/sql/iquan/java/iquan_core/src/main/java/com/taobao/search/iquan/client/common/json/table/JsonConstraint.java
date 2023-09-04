package com.taobao.search.iquan.client.common.json.table;

import com.taobao.search.iquan.client.common.json.common.JsonIndex;
import com.taobao.search.iquan.core.api.schema.IndexType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class JsonConstraint {
    @JsonProperty(value = "multi_unique")
    private List<List<String>> multiUniqueKey = new ArrayList<>();

    @JsonProperty(value = "multi_indexes")
    private List<JsonIndex> multiIndexes= new ArrayList<>();


    public JsonIndex getMultiPrimaryKeyIndex() {
        for (JsonIndex jsonIndex : multiIndexes) {
            String indexType = jsonIndex.getIndexType();
            if (IndexType.from(indexType).isPrimaryKey()) {
                return jsonIndex;
            }
        }
        return null;
    }

    public List<List<String>> getMultiUniqueKey() {
        return multiUniqueKey;
    }

    public void setMultiUniqueKey(List<List<String>> multiUniqueKey) {
        this.multiUniqueKey = multiUniqueKey;
    }

    public List<JsonIndex> getMultiIndexes() {
        return multiIndexes;
    }

    public void setMultiIndexes(List<JsonIndex> multiIndexes) {
        this.multiIndexes = multiIndexes;
    }

    public JsonConstraint() {
    }

    @JsonIgnore
    public boolean isValid() {
        for (JsonIndex jsonIndex:multiIndexes) {
            if (!jsonIndex.isValid()) {
                return false;
            }
        }
        return true;
    }
}
