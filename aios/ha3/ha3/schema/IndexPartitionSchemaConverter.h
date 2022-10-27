#pragma once
#include <vector>

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <iquan_jni/api/IquanEnv.h>
#include <indexlib/config/index_partition_schema.h>
#include <iquan_common/catalog/json/Ha3TableDef.h>

BEGIN_HA3_NAMESPACE(schema);

class IndexPartitionSchemaConverter
{
public:
    IndexPartitionSchemaConverter();
    ~IndexPartitionSchemaConverter();
    IndexPartitionSchemaConverter(const IndexPartitionSchemaConverter &) = delete;
    IndexPartitionSchemaConverter& operator=(const IndexPartitionSchemaConverter &) = delete;

    static void convertToIquanHa3Table(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr, iquan::Ha3TableDef &iquanHa3Table);
private:
    static void convertIndexSchemaToIquanHa3TableIndex(const IE_NAMESPACE(config)::IndexSchemaPtr &indexSchemaPtr,
            std::vector<iquan::Ha3IndexDef> &iquanIndexs);
    static void convertAttributeSchemaToIquanHa3TableAttribute(const IE_NAMESPACE(config)::AttributeSchemaPtr &attributeSchemaPtr,
            std::vector<std::string> &iquanAttrs);
    static void convertFieldSchemaToIquanHa3TableField(const IE_NAMESPACE(config)::FieldSchemaPtr &fieldSchemaPtr,
            std::vector<iquan::Ha3FieldDef> &iquanFields);
    static void convertSummarySchemaToIquanHa3TableSummary(const IE_NAMESPACE(config)::SummarySchemaPtr &summarySchemaPtr,
            iquan::Ha3SummaryDef &iquanSummary);

    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IndexPartitionSchemaConverter);
END_HA3_NAMESPACE(schema);
