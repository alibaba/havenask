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
#include "indexlib/config/schema_differ.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SchemaDiffer);

SchemaDiffer::SchemaDiffer() {}

SchemaDiffer::~SchemaDiffer() {}

bool SchemaDiffer::CheckAlterFields(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                    string& errorMsg)
{
    if (newSchema->HasModifyOperations()) {
        oldSchema->EnableThrowAssertCompatibleException();
        try {
            oldSchema->AssertCompatible(*newSchema);
        } catch (const AssertCompatibleException& e) {
            errorMsg = string("Schema with modify operation is not compatible: ") + e.GetMessage();
            return false;
        } catch (const ExceptionBase& e) {
            errorMsg = string("Schema with modify operation assertCompatiable Exception: ") + e.GetMessage();
            return false;
        }
    }

    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    vector<AttributeConfigPtr> deleteAttributes;
    vector<IndexConfigPtr> deleteIndexes;
    if (!CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg) ||
        !CalculateDeleteFields(oldSchema, newSchema, deleteAttributes, deleteIndexes, errorMsg)) {
        return false;
    }
    return true;
}

bool SchemaDiffer::CalculateDeleteFields(const IndexPartitionSchemaPtr& oldSchema,
                                         const IndexPartitionSchemaPtr& newSchema,
                                         std::vector<AttributeConfigPtr>& deleteAttributes,
                                         std::vector<IndexConfigPtr>& deleteIndexes, std::string& errorMsg)
{
    auto oldAttrSchema = oldSchema->GetAttributeSchema();
    auto newAttrSchema = newSchema->GetAttributeSchema();
    GetAlterFields(newAttrSchema, oldAttrSchema, deleteAttributes);
    auto oldIndexSchema = oldSchema->GetIndexSchema();
    auto newIndexSchema = newSchema->GetIndexSchema();
    GetAlterFields(newIndexSchema, oldIndexSchema, deleteIndexes);

    auto oldSummarySchema = oldSchema->GetSummarySchema();
    auto newSummarySchema = newSchema->GetSummarySchema();
    // check pack attribute not support delete, attribute in schema not support
    for (size_t i = 0; i < deleteAttributes.size(); i++) {
        if (deleteAttributes[i]->GetPackAttributeConfig()) {
            stringstream ss;
            ss << "not support delete pack attribute:" << deleteAttributes[i]->GetAttrName();
            errorMsg = ss.str();
            IE_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }

        string deleteAttrName = deleteAttributes[i]->GetAttrName();
        if (oldSummarySchema && oldSummarySchema->GetSummaryConfig(deleteAttrName) && newSummarySchema &&
            newSummarySchema->GetSummaryConfig(deleteAttrName)) {
            // attribute in summary, only delete attrbute, not delete summary
            stringstream ss;
            ss << "not support delete attribute in summary:" << deleteAttributes[i]->GetAttrName();
            errorMsg = ss.str();
            IE_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    // check pk index, trie index not support delete
    for (size_t i = 0; i < deleteIndexes.size(); i++) {
        InvertedIndexType type = deleteIndexes[i]->GetInvertedIndexType();
        if (type == it_primarykey64 || type == it_primarykey128 || type == it_trie) {
            stringstream ss;
            ss << "not support delete index " << deleteIndexes[i]->GetIndexName() << "type ["
               << IndexConfig::InvertedIndexTypeToStr(type) << "]";
            errorMsg = ss.str();
            IE_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    // check summary field not support delete
    vector<std::shared_ptr<SummaryConfig>> deleteSummarys;
    GetAlterFields(newSchema->GetSummarySchema(), oldSchema->GetSummarySchema(), deleteSummarys);
    for (size_t i = 0; i < deleteSummarys.size(); i++) {
        if (oldAttrSchema && oldAttrSchema->GetAttributeConfig(deleteSummarys[i]->GetSummaryName())) {
            continue;
        }
        errorMsg = "not support delete summary";
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

bool SchemaDiffer::CalculateAlterFields(const IndexPartitionSchemaPtr& oldSchema,
                                        const IndexPartitionSchemaPtr& newSchema,
                                        vector<AttributeConfigPtr>& alterAttributes,
                                        vector<IndexConfigPtr>& alterIndexes, string& errorMsg)
{
    if (!CheckSchemaMeta(oldSchema, newSchema, errorMsg)) {
        IE_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    auto oldAttrSchema = oldSchema->GetAttributeSchema();
    auto newAttrSchema = newSchema->GetAttributeSchema();
    auto oldIndexSchema = oldSchema->GetIndexSchema();
    auto newIndexSchema = newSchema->GetIndexSchema();
    vector<std::shared_ptr<SummaryConfig>> alterSummarys;
    GetAlterFields(oldAttrSchema, newAttrSchema, alterAttributes);
    GetAlterFields(oldIndexSchema, newIndexSchema, alterIndexes);
    GetAlterFields(oldSchema->GetSummarySchema(), newSchema->GetSummarySchema(), alterSummarys);

    if (!CheckAlterFields(alterIndexes, alterAttributes, alterSummarys, newSchema, errorMsg)) {
        IE_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

size_t SchemaDiffer::GetAlterFields(const AttributeSchemaPtr& oldSchema, const AttributeSchemaPtr& newSchema,
                                    vector<AttributeConfigPtr>& alterFields)
{
    if (!newSchema) {
        return 0;
    }

    for (auto iter = newSchema->Begin(); iter != newSchema->End(); iter++) {
        if ((*iter)->IsDeleted()) {
            continue;
        }

        if (!oldSchema) {
            alterFields.push_back(*iter);
            continue;
        }

        auto oldConfig = oldSchema->GetAttributeConfig((*iter)->GetAttrName());
        if (!oldConfig) {
            alterFields.push_back(*iter);
            continue;
        }

        if (newSchema->IsDeleted(oldConfig->GetAttrId())) {
            alterFields.push_back(*iter);
            continue;
        }
    }
    return alterFields.size();
}

size_t SchemaDiffer::GetAlterFields(const IndexSchemaPtr& oldSchema, const IndexSchemaPtr& newSchema,
                                    vector<IndexConfigPtr>& alterFields)
{
    if (!newSchema) {
        return 0;
    }
    for (auto iter = newSchema->Begin(); iter != newSchema->End(); iter++) {
        if ((*iter)->IsDeleted()) {
            continue;
        }

        if ((*iter)->GetShardingType() == IndexConfig::IST_IS_SHARDING) {
            continue;
        }

        if (!oldSchema) {
            alterFields.push_back(*iter);
            continue;
        }

        auto oldConfig = oldSchema->GetIndexConfig((*iter)->GetIndexName());
        if (!oldConfig) {
            alterFields.push_back(*iter);
            continue;
        }

        if (newSchema->IsDeleted(oldConfig->GetIndexId())) {
            alterFields.push_back(*iter);
            continue;
        }
    }
    return alterFields.size();
}

size_t SchemaDiffer::GetAlterFields(const SummarySchemaPtr& oldSchema, const SummarySchemaPtr& newSchema,
                                    vector<std::shared_ptr<SummaryConfig>>& alterSummarys)
{
    if (!newSchema) {
        return 0;
    }
    for (auto iter = newSchema->Begin(); iter != newSchema->End(); iter++) {
        std::shared_ptr<SummaryConfig> conf = *iter;
        if (!oldSchema || !oldSchema->GetSummaryConfig(conf->GetSummaryName())) {
            alterSummarys.push_back(conf);
        }
    }
    return alterSummarys.size();
}

bool SchemaDiffer::CheckSchemaMeta(const IndexPartitionSchemaPtr& oldSchema, const IndexPartitionSchemaPtr& newSchema,
                                   string& errorMsg)
{
    assert(oldSchema);
    assert(newSchema);
    if (oldSchema->GetSchemaVersionId() >= newSchema->GetSchemaVersionId()) {
        errorMsg = "not support update schema with oldSchema versionId [" +
                   StringUtil::toString(oldSchema->GetSchemaVersionId()) + "] >= newSchema versionId [" +
                   StringUtil::toString(newSchema->GetSchemaVersionId()) + "]";
        return false;
    }

    if (oldSchema->GetTableType() != newSchema->GetTableType()) {
        errorMsg = "not support update schema with different table type."
                   "old schema [" +
                   IndexPartitionSchema::TableType2Str(oldSchema->GetTableType()) + "], new schema [" +
                   IndexPartitionSchema::TableType2Str(newSchema->GetTableType()) + "]";
        return false;
    }

    if (oldSchema->GetSchemaName() != newSchema->GetSchemaName()) {
        errorMsg = "not support update schema with different table name."
                   "old schema [" +
                   oldSchema->GetSchemaName() + "], new schema [" + newSchema->GetSchemaName() + "]";
        return false;
    }

    if (oldSchema->GetTableType() != tt_index) {
        errorMsg = "not support update schema for [" + IndexPartitionSchema::TableType2Str(oldSchema->GetTableType()) +
                   "] table";
        return false;
    }
    if (!newSchema->HasModifyOperations()) {
        if (oldSchema->GetSubIndexPartitionSchema() || newSchema->GetSubIndexPartitionSchema()) {
            errorMsg = "not support update schema for sub doc schema, table [" + oldSchema->GetSchemaName() + "]";
            return false;
        }
    } else {
        auto oldSubSchema = oldSchema->GetSubIndexPartitionSchema();
        auto newSubSchema = newSchema->GetSubIndexPartitionSchema();
        if (newSubSchema && newSubSchema->HasModifyOperations()) {
            errorMsg = "not support modify sub schema, table [" + oldSchema->GetSchemaName() + "]";
            return false;
        }
        if ((oldSubSchema && !newSubSchema) || (!oldSubSchema && newSubSchema)) {
            errorMsg = "old schema & new schema must both have subSchema"
                       " or haven't subSchema!, table [" +
                       oldSchema->GetSchemaName() + "]";
            return false;
        }
    }
    return true;
}

bool SchemaDiffer::CheckAlterFields(const vector<IndexConfigPtr>& alterIndexes,
                                    const vector<AttributeConfigPtr>& alterAttributes,
                                    const vector<std::shared_ptr<SummaryConfig>>& alterSummarys,
                                    const IndexPartitionSchemaPtr& newSchema, string& errorMsg)
{
    auto newAttrSchema = newSchema->GetAttributeSchema();
    for (auto index : alterIndexes) {
        InvertedIndexType indexType = index->GetInvertedIndexType();
        if (indexType == it_primarykey64 || indexType == it_primarykey128 || indexType == it_trie) {
            errorMsg = "not support add new index [" + index->GetIndexName() + "] which is primary key index!";
            return false;
        }
        if (index->HasTruncate()) {
            errorMsg = "not support add new index [" + index->GetIndexName() + "] which need truncate!";
            return false;
        }
    }
    for (auto attr : alterAttributes) {
        if (attr->GetPackAttributeConfig()) {
            errorMsg = "not support add new attribute [" + attr->GetAttrName() + "] which in pack attribute!";
            return false;
        }
    }
    for (auto summary : alterSummarys) {
        if (!newAttrSchema || !newAttrSchema->GetAttributeConfig(summary->GetSummaryName())) {
            errorMsg = "not support add new summary field [" + summary->GetSummaryName() + "]";
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::config
