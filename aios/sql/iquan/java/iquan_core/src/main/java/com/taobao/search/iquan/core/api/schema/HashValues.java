package com.taobao.search.iquan.core.api.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

public class HashValues {
    //from hint
    private final String assignedPartitionIds;
    //from condition
    private final Map<String, String> hashValuesSepMap;
    private String assignedHashValues;
    private Map<String, Set<String>> hashValuesMap;

    public HashValues(String assignedPartitionIds,
                      String assignedHashValues,
                      Map<String, String> hashValuesSepMap,
                      Map<String, Set<String>> hashValuesMap) {
        if (!hashValuesSepMap.isEmpty() && (hashValuesSepMap.size() != hashValuesMap.size())) {
            throw new RuntimeException("hashValuesSepMap size must be equal hashValuesMap size");
        }
        this.assignedPartitionIds = assignedPartitionIds;
        this.assignedHashValues = assignedHashValues;
        this.hashValuesSepMap = hashValuesSepMap;
        this.hashValuesMap = hashValuesMap;
    }

    public boolean isEmpty() {
        if (StringUtils.isBlank(assignedPartitionIds)
                && StringUtils.isBlank(assignedHashValues)
                && hashValuesMap.isEmpty()) {
            return true;
        }
        return false;
    }

    public boolean merge(List<MutablePair<Integer, String>> posHashFields, HashValues other) {
        if (this.isEmpty() && other.isEmpty()) {
            return true;
        }
        if (this.isEmpty() || other.isEmpty()) {
            return false;
        }
        //priority : assignedPartitionIds > assignedHashValues > hashValuesMap
        //assignedPartitionIds and assignedHashValues can not be merged
        if (StringUtils.equals(assignedPartitionIds, other.assignedPartitionIds)) {
            if (StringUtils.isNotBlank(assignedPartitionIds) || StringUtils.isNotBlank(other.assignedPartitionIds)) {
                assignedHashValues = null;
                hashValuesMap.clear();
                hashValuesSepMap.clear();
                return true;
            }
        } else {
            return false;
        }
        if (StringUtils.equals(assignedHashValues, other.assignedHashValues)) {
            if (StringUtils.isNotBlank(assignedHashValues) || StringUtils.isNotBlank(other.assignedHashValues)) {
                hashValuesMap.clear();
                hashValuesSepMap.clear();
                return true;
            }
        } else {
            return false;
        }

        //merge hashValuesMap
        String hashField = PlanWriteUtils.formatFieldName(posHashFields.get(0).getRight());
        hashValuesMap.keySet().removeIf(key -> !key.equals(hashField));
        if (hashValuesMap.containsKey(hashField) && other.hashValuesMap.containsKey(hashField)) {
            if (!StringUtils.equals(hashValuesSepMap.get(hashField), other.hashValuesSepMap.get(hashField))) {
                throw new RuntimeException(hashField + " separator must be equal");
            }
            hashValuesMap.get(hashField).addAll(other.hashValuesMap.get(hashField));
            return true;
        } else if (!hashValuesMap.containsKey(hashField) && !other.hashValuesMap.containsKey(hashField)) {
            return true;
        } else {
            return false;
        }
    }

    public String getAssignedPartitionIds() {
        return assignedPartitionIds;
    }

    public String getAssignedHashValues() {
        return assignedHashValues;
    }

    public Map<String, String> getHashValuesSepMap() {
        return hashValuesSepMap;
    }

    public Map<String, Set<String>> getHashValuesMap() {
        return hashValuesMap;
    }

    public void setHashValuesMap(Map<String, Set<String>> hashValuesMap) {
        this.hashValuesMap = hashValuesMap;
    }

    public HashValues copy() {
        Map<String, Set<String>> newHashValuesMap = new TreeMap<>();
        Map<String, String> newHashValuesSepMap = new TreeMap<>(hashValuesSepMap);
        for (Map.Entry<String, Set<String>> entry : hashValuesMap.entrySet()) {
            newHashValuesMap.put(entry.getKey(), new TreeSet<>(entry.getValue()));
        }
        return new HashValues(assignedPartitionIds, assignedHashValues, newHashValuesSepMap, newHashValuesMap);
    }

    public String getDigest() {
        StringBuilder stringBuilder = new StringBuilder(256);
        for (Map.Entry<String, Set<String>> entry : hashValuesMap.entrySet()) {
            stringBuilder.append(entry.getKey()).append(":").append("<");
            for (String value : entry.getValue()) {
                stringBuilder.append(value).append(",");
            }
            stringBuilder.append(">,");
        }
        return String.format("assignedPartitionIds(%s), assignedHashValues(%s), hashValuesMap(%s)",
                assignedPartitionIds, assignedHashValues, stringBuilder);
    }
}
