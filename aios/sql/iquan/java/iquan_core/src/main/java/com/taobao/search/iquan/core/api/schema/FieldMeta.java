package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.core.common.ConstantDefine;

import java.util.Map;
import java.util.TreeMap;

public class FieldMeta {
    public String fieldName = "";
    public String originFieldName = "";
    public FieldType fieldType = FieldType.FT_INVALID;
    public String indexName = "";
    public IndexType indexType = IndexType.IT_NONE;
    public Boolean isAttribute = true;

    public Map<String, String> toMap() {
        Map<String, String> map = new TreeMap<>();
        map.put(ConstantDefine.FIELD_NAME, fieldName);
        map.put(ConstantDefine.FIELD_TYPE, fieldType.getName());
        map.put(ConstantDefine.IS_ATTRIBUTE, isAttribute.toString());
        if (!indexName.isEmpty() && indexType != IndexType.IT_NONE) {
            map.put(ConstantDefine.INDEX_NAME, indexName);
            map.put(ConstantDefine.INDEX_TYPE, indexType.getName());
        }
        return map;
    }
}
