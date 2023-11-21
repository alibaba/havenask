package com.taobao.search.iquan.core.api.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.taobao.search.iquan.client.common.json.catalog.IquanLocation;
import com.taobao.search.iquan.client.common.json.catalog.JsonTableIdentity;
import com.taobao.search.iquan.client.common.json.common.JsonIndexes;
import com.taobao.search.iquan.client.common.json.table.JsonTable;
import com.taobao.search.iquan.core.api.exception.CatalogException;
import com.taobao.search.iquan.core.catalog.GlobalCatalog;
import com.taobao.search.iquan.core.common.ConstantDefine;
import com.taobao.search.iquan.core.rel.plan.PlanWriteUtils;
import com.taobao.search.iquan.core.utils.IquanRelOptUtils;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
public class IquanTable {
    private static final Logger logger = LoggerFactory.getLogger(IquanTable.class);

    private long version = -1;
    private String catalogName;
    private String dbName;
    private String tableName = null;
    private String originalName = null;
    private TableType tableType = TableType.TT_INVALID;
    private List<AbstractField> fields = null;
    private Distribution distribution = null;
    private List<SortDesc> sortDescs = null;
    private Map<String, String> properties = new HashMap<>();
    private JsonTable jsonTable;
    // private JsonLayerTable jsonLayerTable;
    // calc internal
    private final List<FieldMeta> fieldMetaList = new ArrayList<>();
    private Map<String, IndexType> fieldToIndexMap = null;
    private Map<String, FieldMeta> primaryMap = new HashMap<>();
    private JsonIndexes indexes = null;
    private int subTablesCnt = 0;

    public void setTableType(String tableType) {
        this.tableType = TableType.from(tableType);
    }

    private void addField(AbstractField field) {
        if (this.fields == null) {
            this.fields = new ArrayList<>();
        }
        this.fields.add(field);
    }

    private void addProperties(Map<String, String> properties) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.putAll(properties);
    }

    public String getDigest() {
        Map<String, Object> map = new HashMap<>();
        map.put(ConstantDefine.CATALOG_NAME, catalogName);
        map.put(ConstantDefine.DATABASE_NAME, dbName);
        map.put(ConstantDefine.TABLE_VERSION, 1);
// <<<<<<< Updated upstream
        if (tableType.equals(TableType.TT_LAYER_TABLE)) {
            map.put(ConstantDefine.TABLE_TYPE, TableType.TT_LAYER_TABLE);
        } else {
            map.put(ConstantDefine.CONTENT, jsonTable.getJsonTableContent());
        }
// =======
//         if (jsonTable != null) {
//             map.put(ConstantDefine.CONTENT, jsonTable.getJsonTableContent());
//         }
//         if (jsonLayerTable != null) {
//             map.put(ConstantDefine.CONTENT, jsonLayerTable.getJsonLayerTableContent());
//         }
// >>>>>>> Stashed changes

        return IquanRelOptUtils.toJson(map);
    }

    public String getDebugDigest() {
        Map<String, Object> map = new HashMap<>();
        map.put(ConstantDefine.CATALOG_NAME, catalogName);
        map.put(ConstantDefine.DATABASE_NAME, dbName);
        map.put(ConstantDefine.CONTENT, jsonTable);

        return IquanRelOptUtils.toJson(map);
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public boolean isValid() {
        if (tableName == null || tableName.isEmpty() || !tableType.isValid() || fields == null || fields.isEmpty()) {
            logger.error("table is not valid: {}", this.getDigest());
            return false;
        }

        if (!tableType.equals(TableType.TT_LAYER_TABLE)) {
            if (distribution == null || !distribution.isValid()) {
                logger.error("distribution is not valid: {}", this.getDigest());
                return false;
            }
        }

        for (AbstractField field : fields) {
            if (!field.isValid()) {
                logger.error("field is not valid: field {}, table {}", field.getDigest(), this.getDigest());
                return false;
            }
        }
        return true;
    }

    private void postProcess() {
        for (AbstractField field : fields) {
            fillFieldMeta(field, 0);
        }
        fieldToIndexMap = new HashMap<>(fieldMetaList.size() * 2);
        for (FieldMeta fieldMeta : fieldMetaList) {
            fieldToIndexMap.put(fieldMeta.originFieldName, fieldMeta.indexType);
            switch (fieldMeta.indexType) {
                case IT_PRIMARY_KEY:
                case IT_PRIMARYKEY64:
                case IT_PRIMARYKEY128:
                    primaryMap.put(fieldMeta.fieldName, fieldMeta);
                    break;
                default:
                    break;
            }
        }
    }

    private void fillFieldMeta(AbstractField field, int curLevel) {
        if (curLevel > 1) {
            return;
        }

        if (field.isAtomicType()) {
            AtomicField atomicField = (AtomicField) field;
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(atomicField.getFieldName());
            fieldInfo.originFieldName = atomicField.getFieldName();
            fieldInfo.fieldType = atomicField.getFieldType();
            fieldInfo.indexName = atomicField.getIndexName();
            fieldInfo.indexType = atomicField.getIndexType();
            fieldInfo.isAttribute = atomicField.getIsAttribute();
            fieldMetaList.add(fieldInfo);
            return;
        } else if (field.isRowType()) {
            List<AbstractField> recordFields = ((RowField) field).getFields();
            for (AbstractField recordField : recordFields) {
                fillFieldMeta(recordField, curLevel + 1);
            }
        } else if (field.isArrayType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = field.getIsAttribute();
            AbstractField elementField = ((ArrayField) field).getElementField();
            if (elementField.isAtomicType()) {
                AtomicField atomicField = (AtomicField) elementField;
                fieldInfo.indexName = atomicField.getIndexName();
                fieldInfo.indexType = atomicField.getIndexType();
                fieldInfo.isAttribute = atomicField.getIsAttribute();
                fieldMetaList.add(fieldInfo);
            }
        } else if (field.isMultiSetType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = false;
            fieldMetaList.add(fieldInfo);
            fillFieldMeta(((MultiSetField) field).getElementField(), curLevel);
        } else if (field.isMapType()) {
            FieldMeta fieldInfo = new FieldMeta();
            fieldInfo.fieldName = PlanWriteUtils.formatFieldName(field.getFieldName());
            fieldInfo.originFieldName = field.getFieldName();
            fieldInfo.fieldType = field.getFieldType();
            fieldInfo.isAttribute = field.getIsAttribute();
            fieldMetaList.add(fieldInfo);
            return;
        }
    }

    public String getPkIndexName() {
        for (FieldMeta fieldMeta : fieldMetaList) {
            if (fieldMeta.indexType.isPrimaryKey()) {
                return fieldMeta.indexName;
            }
        }
        return null;
    }

    public List<Location> getLocations(GlobalCatalog catalog) {
        if (!catalog.tableExists(dbName, tableName)) {
            throw new CatalogException(String.format("Table [%s.%s] not in catalog [%s]!", dbName, tableName, catalogName));
        }
        JsonTableIdentity tableIdentity = new JsonTableIdentity(dbName, tableName);
        List<IquanLocation> iquanLocations = catalog.getLocationNodeManager().getLocationByTable(tableIdentity);
        if (iquanLocations.isEmpty()) {
            logger.info(String.format("Table [%s.%s.%s] has no locations! use unknown as default", catalogName, dbName, tableName));
            return Collections.singletonList(Location.UNKNOWN);
        }

        return iquanLocations.stream().map(Location::new).collect(Collectors.toList());
    }

    public JsonTableIdentity getJsonTableIdentity() {
        return new JsonTableIdentity(dbName, tableName);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(IquanTable iquanTable) {
        return new Builder(iquanTable);
    }

    public static class Builder {
        private IquanTable iquanTable;
        private boolean deepCopy = false;

        public Builder() {
            iquanTable = new IquanTable();
        }

        public Builder(IquanTable iquanTable) {
            this();

            this.iquanTable.version = iquanTable.getVersion();
            this.iquanTable.tableName = iquanTable.getTableName();
            this.iquanTable.catalogName = iquanTable.getCatalogName();
            this.iquanTable.dbName = iquanTable.getDbName();
            this.iquanTable.originalName = iquanTable.getOriginalName();
            this.iquanTable.tableType = iquanTable.getTableType();
            this.iquanTable.fields = iquanTable.getFields();
            this.iquanTable.distribution = iquanTable.getDistribution();
            this.iquanTable.sortDescs = iquanTable.getSortDescs();
            this.iquanTable.properties = iquanTable.getProperties();
            this.iquanTable.jsonTable = iquanTable.getJsonTable();
            this.deepCopy = true;
            this.iquanTable.subTablesCnt = iquanTable.getSubTablesCnt();
        }

        public Builder version(long version) {
            iquanTable.setVersion(version);
            return this;
        }

        public Builder tableName(String tableName) {
            iquanTable.setTableName(tableName);
            return this;
        }

        public Builder catalogName(String catalogName) {
            iquanTable.setCatalogName(catalogName);
            return this;
        }

        public Builder dbName(String dbName) {
            iquanTable.setDbName(dbName);
            return this;
        }

        public Builder originalName(String originalName) {
            iquanTable.setOriginalName(originalName);
            return this;
        }

        public Builder subTablesCnt(int cnt) {
            iquanTable.setSubTablesCnt(cnt);
            return this;
        }

        public Builder tableType(String tableType) {
            if (!deepCopy) {
                iquanTable.setTableType(tableType);
            }
            return this;
        }

        public Builder fields(List<AbstractField> fields) {
            if (!deepCopy) {
                iquanTable.setFields(fields);
            }
            return this;
        }

        public Builder addField(AbstractField field) {
            if (!deepCopy) {
                iquanTable.addField(field);
            }
            return this;
        }

        public Builder distribution(Distribution distribution) {
            if (!deepCopy) {
                iquanTable.setDistribution(distribution);
            }
            return this;
        }


        public Builder sortDescs(List<SortDesc> sortDescs) {
            if (!deepCopy) {
                iquanTable.setSortDescs(sortDescs);
            }
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            if (!deepCopy) {
                iquanTable.setProperties(properties);
            }
            return this;
        }

        public Builder addProperties(Map<String, String> properties) {
            if (!deepCopy) {
                iquanTable.addProperties(properties);
            }
            return this;
        }

        public Builder jsonTable(JsonTable jsonTable) {
            if (!deepCopy) {
                iquanTable.setJsonTable(jsonTable);
            }
            return this;
        }

        // public Builder jsonLayerTable(JsonLayerTable jsonLayerTable) {
        //     if (!deepCopy) {
        //         iquanTable.setJsonLayerTable(jsonLayerTable);
        //     }
        //     return this;
        // }

        public Builder indexes(JsonIndexes indexes) {
            if (!deepCopy) {
                iquanTable.setIndexes(indexes);
            }
            return this;
        }

        public IquanTable build() {
            if (iquanTable.isValid()) {
                iquanTable.postProcess();
                return iquanTable;
            }
            logger.error("build table fail: {}", iquanTable.getDigest());
            return null;
        }
    }
}
