package com.taobao.search.iquan.client.common.json.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.SqlKind;

public class JsonAggregationIndex extends JsonIndex {
    @JsonProperty("value_fields")
    private List<String> valueFileds = new ArrayList<>();

    public List<String> getValueFileds() {return this.valueFileds;  }

    public Boolean match(List<Map.Entry<String, String>> equalList, List<Map.Entry<String, String>> fromList, List<Map.Entry<String, String>> toList, List<String> needFields) {
        if (!match(equalList, needFields)) {
            return Boolean.FALSE;
        }
        return fromList.stream().map(Map.Entry::getKey).allMatch(getValueFileds()::contains)
            && toList.stream().map(Map.Entry::getKey).allMatch(getValueFileds()::contains);

    }

    public Boolean match(List<Map.Entry<String, String>> conditions, List<String> needFields) {
        if (!match(conditions)) {
            return Boolean.FALSE;
        }
       return needFields.stream().allMatch(field -> getValueFileds().contains(field) || getIndexFields().contains(field));
    }

    public Boolean match(List<Map.Entry<String, String>> conditions) {
        List<String> conditionKeys = conditions.stream().map(Map.Entry::getKey).sorted().collect(Collectors.toList());
        List<String> indexKeys = getIndexFields().stream().sorted().collect(Collectors.toList());
        return conditionKeys.equals(indexKeys);
    }

    public List<String> getAggregationKeys(List<Map.Entry<String, String>> conditions) {
        Map<String, String> map = conditions.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return getIndexFields().stream().map(map::get).collect(Collectors.toList());
    }
}
