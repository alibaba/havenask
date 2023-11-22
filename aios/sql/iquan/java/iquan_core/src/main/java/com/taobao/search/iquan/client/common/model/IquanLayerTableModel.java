package com.taobao.search.iquan.client.common.model;

import java.util.Map;
import java.util.TreeMap;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.table.JsonLayerTableContent;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;

public class IquanLayerTableModel extends IquanModelBase {
    private String layerTableName = ConstantDefine.EMPTY;
    private String content = ConstantDefine.EMPTY;
    private JsonLayerTableContent contentObj = null;
    @Override
    public boolean isQualifierValid() {
        return false;
    }

    @Override
    public boolean isPathValid() {
        return false;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    public String getLayerTableName() {
        return layerTableName;
    }

    public void setLayerTableName(String layerTableName) {
        this.layerTableName = layerTableName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    private void parseContent() {
        if (contentObj != null) {
            return;
        }
        contentObj = IquanRelOptUtils.fromJson(content, JsonLayerTableContent.class);
    }

    public JsonLayerTableContent getContentObj() {
        parseContent();
        return contentObj;
    }

    @Override
    public String getDigest() {
        return String.format("%s:%s, %s:%s, %s:%s, %s:%s",
                ConstantDefine.CATALOG_NAME, catalog_name,
                ConstantDefine.DATABASE_NAME, database_name,
                ConstantDefine.LAYER_TABLE_NAME, layerTableName,
                ConstantDefine.CONTENT, content);
    }

    public String getDetailInfo() {
        Map<String, Object> maps = new TreeMap<>();

        maps.put("catalog_name", getCatalog_name());
        maps.put("database_name", getDatabase_name());

        if (contentObj == null) {
            parseContent();
        }
        maps.put("content", contentObj);
        return IquanRelOptUtils.toJson(maps);
    }

    public static IquanLayerTableModel createFromMap(Map<String, Object> map, String catalogName, String databaseName) {
        IquanLayerTableModel layerTableModel = new IquanLayerTableModel();

        String layerTableName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.LAYER_TABLE_NAME, ConstantDefine.EMPTY);
        String content = ConstantDefine.EMPTY;
        if (map.containsKey(ConstantDefine.CONTENT)) {
            Object contentObj = map.get(ConstantDefine.CONTENT);
            if (contentObj instanceof String) {
                content = (String) contentObj;
            } else {
                content = IquanRelOptUtils.toJson(contentObj);
            }
        }

        layerTableModel.setLayerTableName(layerTableName);
        layerTableModel.setCatalog_name(catalogName);
        layerTableModel.setDatabase_name(databaseName);
        layerTableModel.setContent(content);

        return layerTableModel;
    }
}
