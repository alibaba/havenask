#include <ha3/schema/IndexPartitionSchemaConverter.h>
#include <autil/legacy/json.h>
#include <autil/legacy/any_jsonizable.h>

using namespace std;
using autil::legacy::json::JsonMap;

IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(schema);
HA3_LOG_SETUP(schema, IndexPartitionSchemaConverter);

IndexPartitionSchemaConverter::IndexPartitionSchemaConverter() {
}

IndexPartitionSchemaConverter::~IndexPartitionSchemaConverter() {
}

void IndexPartitionSchemaConverter::convertToIquanHa3Table(const IndexPartitionSchemaPtr &schemaPtr, iquan::Ha3TableDef &iquanHa3Table) {
    iquanHa3Table.tableName = schemaPtr->GetSchemaName();
    iquanHa3Table.tableType = IndexPartitionSchema::TableType2Str(schemaPtr->GetTableType());

    // indexs
    auto &indexSchemaPtr = schemaPtr->GetIndexSchema();
    convertIndexSchemaToIquanHa3TableIndex(indexSchemaPtr, iquanHa3Table.indexes);

    // attrs
    auto &attributeSchemaPtr = schemaPtr->GetAttributeSchema();
    convertAttributeSchemaToIquanHa3TableAttribute(attributeSchemaPtr, iquanHa3Table.attrs);

    // fields
    auto &fieldSchemaPtr = schemaPtr->GetFieldSchema();
    convertFieldSchemaToIquanHa3TableField(fieldSchemaPtr, iquanHa3Table.fields);

    // customTableConfDef
    auto &customizedTableConfigPtr = schemaPtr->GetCustomizedTableConfig();
    if (customizedTableConfigPtr.get() != nullptr) {
        iquanHa3Table.customTableConfDef.identifier = customizedTableConfigPtr->GetIdentifier();
        iquanHa3Table.customTableConfDef.pluginName = customizedTableConfigPtr->GetPluginName();
        iquanHa3Table.customTableConfDef.parameters = customizedTableConfigPtr->GetParameters();
    }

    // subTableDef
    auto &subSchemaPtr = schemaPtr->GetSubIndexPartitionSchema();
    if (subSchemaPtr.get() != nullptr) {
        iquanHa3Table.subTableDef.tableName = subSchemaPtr->GetSchemaName();

        // subTable indexs
        auto &indexSchemaPtr = subSchemaPtr->GetIndexSchema();
        convertIndexSchemaToIquanHa3TableIndex(indexSchemaPtr, iquanHa3Table.subTableDef.indexes);

        // subTable attrs
        auto &attributeSchemaPtr = subSchemaPtr->GetAttributeSchema();
        convertAttributeSchemaToIquanHa3TableAttribute(attributeSchemaPtr, iquanHa3Table.subTableDef.attrs);

        // subTable fields
        auto &fieldSchemaPtr = subSchemaPtr->GetFieldSchema();
        convertFieldSchemaToIquanHa3TableField(fieldSchemaPtr, iquanHa3Table.subTableDef.fields);
    }

    // Summary schema
    auto &summarySchemaPtr = schemaPtr->GetSummarySchema();
    convertSummarySchemaToIquanHa3TableSummary(summarySchemaPtr, iquanHa3Table.summary);
}

void IndexPartitionSchemaConverter::convertIndexSchemaToIquanHa3TableIndex(const IndexSchemaPtr &indexSchemaPtr,
        std::vector<iquan::Ha3IndexDef> &iquanIndexs) {
    if (indexSchemaPtr.get() == nullptr) {
        return;
    }
    for (auto indexConfigItr = indexSchemaPtr->Begin(); indexConfigItr != indexSchemaPtr->End(); ++indexConfigItr) {
        iquan::Ha3IndexDef ha3IndexDef;
        ha3IndexDef.indexName = (*indexConfigItr)->GetIndexName();
        ha3IndexDef.indexType = IndexConfig::IndexTypeToStr((*indexConfigItr)->GetIndexType());

        autil::legacy::json::JsonMap indexConfigJsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(autil::legacy::ToJson(*(*indexConfigItr)));
        static const std::string KEY_INDEX_FIELDS = "index_fields";
        if (indexConfigJsonMap.find(KEY_INDEX_FIELDS) != indexConfigJsonMap.end()) {
            ha3IndexDef.indexFields = indexConfigJsonMap[KEY_INDEX_FIELDS];
        }

        iquanIndexs.emplace_back(ha3IndexDef);
    }
}

void IndexPartitionSchemaConverter::convertAttributeSchemaToIquanHa3TableAttribute(const AttributeSchemaPtr &attributeSchemaPtr,
        std::vector<std::string> &iquanAttrs) {
    if (attributeSchemaPtr.get() == nullptr) {
        return;
    }
    for (auto attributeConfigItr = attributeSchemaPtr->Begin(); attributeConfigItr != attributeSchemaPtr->End(); ++attributeConfigItr) {
        iquanAttrs.emplace_back((*attributeConfigItr)->GetAttrName());
    }
}

void IndexPartitionSchemaConverter::convertFieldSchemaToIquanHa3TableField(const FieldSchemaPtr &fieldSchemaPtr,
        std::vector<iquan::Ha3FieldDef> &iquanFields) {
    if (fieldSchemaPtr.get() == nullptr) {
        return;
    }
    for (auto fieldConfigItr = fieldSchemaPtr->Begin(); fieldConfigItr != fieldSchemaPtr->End(); ++fieldConfigItr) {
        iquan::Ha3FieldDef ha3FieldDef;
        ha3FieldDef.multiValue = (*fieldConfigItr)->IsMultiValue();
        ha3FieldDef.fieldType = FieldConfig::FieldTypeToStr((*fieldConfigItr)->GetFieldType());
        ha3FieldDef.fieldName = (*fieldConfigItr)->GetFieldName();
        iquanFields.emplace_back(ha3FieldDef);
    }
}

void IndexPartitionSchemaConverter::convertSummarySchemaToIquanHa3TableSummary(const SummarySchemaPtr &summarySchemaPtr,
        iquan::Ha3SummaryDef &iquanSummary) {
    if (summarySchemaPtr.get() == nullptr) {
        return;
    }
    for (auto summaryConfigItr = summarySchemaPtr->Begin(); summaryConfigItr != summarySchemaPtr->End(); ++summaryConfigItr) {
        iquanSummary.summaryFields.emplace_back((*summaryConfigItr)->GetSummaryName());
    }
}

END_HA3_NAMESPACE(schema);

