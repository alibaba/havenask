package com.taobao.search.iquan.client.common.json.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.search.iquan.client.common.json.table.JsonJoinInfo;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class IquanLocation {
    @JsonProperty(value = "location_table_name", required = false)
    private String locationTableName;
    @JsonProperty(value = "node_name", required = true)
    private String nodeName;
    @JsonProperty(value = "partition_cnt", required = true)
    private int partitionCnt;
    @JsonProperty(value = "node_type", required = true)
    private String nodeType;
    @JsonProperty(value = "tables", required = false)
    private List<JsonTableIdentity> tableIdentities = new ArrayList<>();
    @JsonProperty(value = "functions")
    private List<JsonFunctionIdentity> functionIdentities = new ArrayList<>();
    @JsonProperty(value = "equivalent_hash_fields", required = false)
    private List<List<JsonFieldIdentity>> fieldIdentities = new ArrayList<>();
    @JsonProperty(value = "join_info", required = false)
    private List<JsonJoinInfo> joinInfos = new ArrayList<>();

    @JsonIgnore
    private Map<JsonTableIdentity, Map<JsonTableIdentity, JsonJoinInfo>> joinInfoMapper = new HashMap<>();

    public IquanLocation(String name, int partitionCnt, String nodeType) {
        this.nodeName = name;
        this.nodeType = nodeType;
        this.partitionCnt = partitionCnt;
    }

    @AllArgsConstructor
    static class SubSetInfo {
        public int parent;
        public int rank;
    }

    @JsonIgnore
    private Integer autoId = 0;

    @JsonIgnore
    private final Map<JsonFieldIdentity, Integer> fieldId = new HashMap<>();

    @JsonIgnore
    private final Map<Integer, SubSetInfo> subSetInfoMap = new HashMap<>();

    public void init() {
        initEquivalentFields();
        initJoinInfoMapper();
    }

    private void initJoinInfoMapper() {
        if (joinInfos == null) {
            return;
        }

        for (JsonJoinInfo jsonJoinInfo : joinInfos) {
            JsonTableIdentity mainTable = jsonJoinInfo.getMainTableIdentity();
            JsonTableIdentity auxTable = jsonJoinInfo.getAuxTableIdentity();
            getJoinInfoMapper().putIfAbsent(mainTable, new HashMap<>());
            Map<JsonTableIdentity, JsonJoinInfo> map = joinInfoMapper.get(mainTable);
            if (map.containsKey(auxTable)) {
                throw new CatalogException(String.format("duplicate joinInfo when register JoinInfo %s", jsonJoinInfo));
            }
            map.put(auxTable, jsonJoinInfo);
        }
    }

    private void initEquivalentFields() {
        if (fieldIdentities == null) {
            return;
        }

        for (List<JsonFieldIdentity> group : fieldIdentities) {
            if (group.size() < 2) {
                continue;
            }
            for (int i = 1; i < group.size(); ++i) {
                addEquivalentFieldPair(group.get(i - 1), group.get(i));
            }
        }
    }

    public boolean isEquivalentFieldPair(JsonFieldIdentity x, JsonFieldIdentity y) {
        int xx = getFieldId(x);
        int yy = getFieldId(y);
        return xx != -1 && yy != -1 && unionFind(xx) == unionFind(yy);
    }

    public JsonJoinInfo getJsonJoinInfo(JsonTableIdentity main, JsonTableIdentity aux) {
        Map<JsonTableIdentity, JsonJoinInfo> auxMap = joinInfoMapper.get(main);
        if (auxMap == null) {
            return null;
        }
        return auxMap.get(aux);
    }

    private void addEquivalentFieldPair(JsonFieldIdentity x, JsonFieldIdentity y) {
        unionSubSet(getFieldIdOrAdd(x), getFieldIdOrAdd(y));
    }

    private Integer getFieldIdOrAdd(JsonFieldIdentity jsonFieldIdentity) {
        if (!fieldId.containsKey(jsonFieldIdentity)) {
            synchronized (autoId) {
                fieldId.put(jsonFieldIdentity, autoId);
                ++autoId;
            }
        }

        return fieldId.get(jsonFieldIdentity);
    }

    private Integer getFieldId(JsonFieldIdentity jsonFieldIdentity) {
        return fieldId.getOrDefault(jsonFieldIdentity, -1);
    }

    private int unionFind(Integer i) {
        if (subSetInfoMap.get(i).parent != i) {
            subSetInfoMap.get(i).parent = unionFind(subSetInfoMap.get(i).parent);
        }
        return subSetInfoMap.get(i).parent;
    }

    private void unionSubSet(Integer i, Integer j) {
        int x = unionFind(i);
        int y = unionFind(j);

        if (subSetInfoMap.get(x).rank < subSetInfoMap.get(y).rank) {
            subSetInfoMap.get(x).parent = y;
        } else if (subSetInfoMap.get(x).rank > subSetInfoMap.get(y).rank) {
            subSetInfoMap.get(y).parent = x;
        } else {
            subSetInfoMap.get(x).parent = y;
            ++subSetInfoMap.get(y).rank;
        }
    }
}
