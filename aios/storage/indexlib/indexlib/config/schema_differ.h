/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_SCHEMA_DIFFER_H
#define __INDEXLIB_SCHEMA_DIFFER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class SchemaDiffer
{
public:
    SchemaDiffer();
    ~SchemaDiffer();

public:
    static bool CalculateAlterFields(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                     std::vector<AttributeConfigPtr>& alterAttributes,
                                     std::vector<IndexConfigPtr>& alterIndexes)
    {
        std::string errorMsg;
        return CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg);
    }

    static bool CalculateAlterFields(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                     std::vector<AttributeConfigPtr>& alterAttributes,
                                     std::vector<IndexConfigPtr>& alterIndexes, std::string& errorMsg);

    static bool CalculateDeleteFields(const IndexPartitionSchemaPtr& oldSchema,
                                      const IndexPartitionSchemaPtr& newSchema,
                                      std::vector<AttributeConfigPtr>& deleteAttributes,
                                      std::vector<IndexConfigPtr>& deleteIndexes, std::string& errorMsg);

    static bool CheckAlterFields(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                 std::string& errorMsg);

private:
    static bool CheckSchemaMeta(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                std::string& errorMsg);

    static bool CheckAlterFields(const std::vector<IndexConfigPtr>& alterIndexes,
                                 const std::vector<AttributeConfigPtr>& alterAttributes,
                                 const std::vector<std::shared_ptr<SummaryConfig>>& alterSummarys,
                                 const IndexPartitionSchemaPtr& newSchema, std::string& errorMsg);

    static size_t GetAlterFields(const AttributeSchemaPtr& oldSchema, const AttributeSchemaPtr& newSchema,
                                 std::vector<AttributeConfigPtr>& alterFields);

    static size_t GetAlterFields(const IndexSchemaPtr& oldSchema, const IndexSchemaPtr& newSchema,
                                 std::vector<IndexConfigPtr>& alterFields);

    static size_t GetAlterFields(const SummarySchemaPtr& oldSchema, const SummarySchemaPtr& newSchema,
                                 std::vector<std::shared_ptr<SummaryConfig>>& alterSummarys);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaDiffer);
}} // namespace indexlib::config

#endif //__INDEXLIB_SCHEMA_DIFFER_H
