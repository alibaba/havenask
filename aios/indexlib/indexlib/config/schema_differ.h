#ifndef __INDEXLIB_SCHEMA_DIFFER_H
#define __INDEXLIB_SCHEMA_DIFFER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(config);

class SchemaDiffer
{
public:
    SchemaDiffer();
    ~SchemaDiffer();
public:
    static bool CalculateAlterFields(
            const IndexPartitionSchemaPtr& oldSchema,
            const IndexPartitionSchemaPtr& newSchema,
            std::vector<AttributeConfigPtr>& alterAttributes,
            std::vector<IndexConfigPtr>& alterIndexes)
    {
        std::string errorMsg;
        return CalculateAlterFields(oldSchema, newSchema,
                alterAttributes, alterIndexes, errorMsg);
    }
    
    static bool CalculateAlterFields(
            const IndexPartitionSchemaPtr& oldSchema,
            const IndexPartitionSchemaPtr& newSchema,
            std::vector<AttributeConfigPtr>& alterAttributes,
            std::vector<IndexConfigPtr>& alterIndexes,
            std::string& errorMsg);

    static bool CalculateDeleteFields(
        const IndexPartitionSchemaPtr& oldSchema,
        const IndexPartitionSchemaPtr& newSchema,
        std::vector<AttributeConfigPtr>& deleteAttributes,
        std::vector<IndexConfigPtr>& deleteIndexes,
        std::string& errorMsg);
    
    static bool CheckAlterFields(
            const IndexPartitionSchemaPtr& oldSchema,
            const IndexPartitionSchemaPtr& newSchema,
            std::string& errorMsg);
    
private:
    static bool CheckSchemaMeta(const IndexPartitionSchemaPtr& oldSchema,
                                const IndexPartitionSchemaPtr& newSchema,
                                std::string& errorMsg);

    static bool CheckAlterFields(const std::vector<IndexConfigPtr>& alterIndexes,
                                 const std::vector<AttributeConfigPtr>& alterAttributes,
                                 const std::vector<SummaryConfigPtr>& alterSummarys,
                                 const IndexPartitionSchemaPtr& newSchema,
                                 std::string& errorMsg);

    static size_t GetAlterFields(const AttributeSchemaPtr& oldSchema,
                                 const AttributeSchemaPtr& newSchema, 
                                 std::vector<AttributeConfigPtr>& alterFields);
    
    static size_t GetAlterFields(const IndexSchemaPtr& oldSchema,
                                 const IndexSchemaPtr& newSchema, 
                                 std::vector<IndexConfigPtr>& alterFields);
    
    static size_t GetAlterFields(const SummarySchemaPtr& oldSchema,
                                 const SummarySchemaPtr& newSchema,
                                 std::vector<SummaryConfigPtr>& alterSummarys);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaDiffer);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SCHEMA_DIFFER_H
