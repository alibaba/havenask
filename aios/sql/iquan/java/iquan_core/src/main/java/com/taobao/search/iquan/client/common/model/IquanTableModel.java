package com.taobao.search.iquan.client.common.model;

import com.taobao.search.iquan.client.common.common.ConstantDefine;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IquanTableModel extends IquanModelBase {
    private static final Logger logger = LoggerFactory.getLogger(IquanTableModel.class);

    private long table_version = 1;
    private String table_name = ConstantDefine.EMPTY;
    private List<String> alias_names = new ArrayList<>();
    private String table_type = ConstantDefine.EMPTY;
    private String table_content_version = ConstantDefine.EMPTY;
    private String table_content = ConstantDefine.EMPTY;

    // for internal use
    private JsonTable contentObj = null;

    public long getTable_version() {
        return table_version;
    }

    public void setTable_version(long table_version) {
        this.table_version = table_version;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public List<String> getAlias_names() {
        return alias_names;
    }

    public void setAlias_names(List<String> alias_names) {
        this.alias_names = alias_names;
    }

    public String getTable_type() {
        return table_type;
    }

    public void setTable_type(String table_type) {
        this.table_type = table_type;
    }

    public String getTable_content_version() {
        return table_content_version;
    }

    public void setTable_content_version(String table_content_version) {
        this.table_content_version = table_content_version;
    }

    public String getTable_content() {
        return table_content;
    }

    public void setTable_content(String table_content) {
        this.table_content = table_content;
    }

    private void parseContent() {
        if (contentObj != null) {
            return;
        }
        contentObj = IquanRelOptUtils.fromJson(table_content, JsonTable.class);
    }

    public JsonTable getContentObj() {
        parseContent();
        return contentObj;
    }

    public IquanTableModel clone() {
        IquanTableModel newModel = new IquanTableModel();
        newModel.table_version = this.table_version;
        newModel.table_name = table_name;
        newModel.table_type = table_type;
        newModel.table_content_version = table_content_version;
        newModel.table_content = table_content;
        return newModel;
    }

    public void updateTableContent() {
        if (contentObj != null) {
            table_content = IquanRelOptUtils.toJson(contentObj);
        }
    }

    @Override
    public boolean isValid() {
        try {
            ExceptionUtils.throwIfTrue(!isPathValid(), "path is not valid");
            ExceptionUtils.throwIfTrue(table_version < 0, "table version is smaller than 0");
            ExceptionUtils.throwIfTrue(table_type.isEmpty(),
                    "table_type is empty");
            ExceptionUtils.throwIfTrue(table_content_version.isEmpty(),
                    "table_coversion.isEmpty()");
            ExceptionUtils.throwIfTrue(table_content.isEmpty(),
                    "table_content is empty");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            return false;
        }

        parseContent();
        if (!table_name.equals(contentObj.getTableName())) {
            logger.error("table_name is not equal contentObj's table name: " + getDigest());
            return false;
        }

        if (!contentObj.isValid()) {
            logger.error("contentObj is not valid");
            return false;
        }
        return true;
    }

    @Override
    public boolean isQualifierValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(catalog_name.isEmpty(), "catalog_name is empty");
            ExceptionUtils.throwIfTrue(database_name.isEmpty(), "database_name is empty");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public boolean isPathValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(!isQualifierValid(), "isQualifierValid is not valid");
            ExceptionUtils.throwIfTrue(table_name.isEmpty(), "table_name is empty");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public String getDigest() {
        return String.format("%s:%s, %s:%s, %s:%d, %s:%s, %s:%s, %s:%s, %s:%s",
                ConstantDefine.CATALOG_NAME, catalog_name,
                ConstantDefine.DATABASE_NAME, database_name,
                ConstantDefine.TABLE_VERSION, table_version,
                ConstantDefine.TABLE_NAME, table_name,
                ConstantDefine.TABLE_TYPE, table_type,
                ConstantDefine.TABLE_CONTENT_VERSION, table_content_version,
                ConstantDefine.TABLE_CONTENT, table_content);
    }

    public String getDetailInfo() {
        Map<String, Object> maps = new TreeMap<>();

        maps.put("catalog_name", getCatalog_name());
        maps.put("database_name", getDatabase_name());
        maps.put("version", getTable_version());

        if (contentObj == null) {
            parseContent();
        }
        maps.put("content", contentObj);
        return IquanRelOptUtils.toJson(maps);
    }

    // utils
    @SuppressWarnings("unchecked")
    public static IquanTableModel createFromMap(Map<String, Object> map) {
        IquanTableModel tableModel = new IquanTableModel();

        String catalogName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.CATALOG_NAME, ConstantDefine.EMPTY);
        String databaseName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.DATABASE_NAME, ConstantDefine.EMPTY);
        long tableVersion = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.TABLE_VERSION, (long) 1);
        String tableName = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.TABLE_NAME, ConstantDefine.EMPTY);
        String tableType = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.TABLE_TYPE, ConstantDefine.EMPTY);
        String tableContentVersion = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.TABLE_CONTENT_VERSION, ConstantDefine.EMPTY);
        List<String> aliasNames = IquanRelOptUtils.getValueFromMap(map, ConstantDefine.ALIAS_NAMES, new ArrayList<>());

        String tableContent = ConstantDefine.EMPTY;
        if (map.containsKey(ConstantDefine.TABLE_CONTENT)) {
            Object tableContentObj = map.get(ConstantDefine.TABLE_CONTENT);
            if (tableContentObj instanceof String) {
                tableContent = (String) tableContentObj;
            } else {
                tableContent = IquanRelOptUtils.toJson(tableContentObj);
            }
        }

        tableModel.setCatalog_name(catalogName);
        tableModel.setDatabase_name(databaseName);
        tableModel.setTable_version(tableVersion);
        tableModel.setTable_name(tableName);
        tableModel.setAlias_names(aliasNames);
        tableModel.setTable_type(tableType);
        tableModel.setTable_content_version(tableContentVersion);
        tableModel.setTable_content(tableContent);

        return tableModel;
    }
}
