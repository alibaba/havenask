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
#include "indexlib/config/mock/FakeTabletSchema.h"

#include <algorithm>
#include <iterator>

#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::config {
FakeTabletSchema::FakeTabletSchema() {}
FakeTabletSchema::~FakeTabletSchema() {}
const std::string& FakeTabletSchema::GetTableType() const { return _tableType; }
const std::string& FakeTabletSchema::GetTableName() const { return _tableName; }
schemaid_t FakeTabletSchema::GetSchemaId() const { return _schemaId; }
std::string FakeTabletSchema::GetSchemaFileName() const
{
    return _schemaId == DEFAULT_SCHEMAID ? SCHEMA_FILE_NAME
                                         : SCHEMA_FILE_NAME + "." + autil::StringUtil::toString(_schemaId);
}
size_t FakeTabletSchema::GetFieldCount() const { return _fieldConfigs.size(); }
fieldid_t FakeTabletSchema::GetFieldId(const std::string& fieldName) const
{
    return GetFieldConfig(fieldName)->GetFieldId();
}
std::shared_ptr<FieldConfig> FakeTabletSchema::GetFieldConfig(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= static_cast<fieldid_t>(_fieldConfigs.size())) {
        return nullptr;
    }
    return _fieldConfigs[fieldId];
}
std::shared_ptr<FieldConfig> FakeTabletSchema::GetFieldConfig(const std::string& fieldName) const
{
    for (const auto& fieldConfig : _fieldConfigs) {
        if (fieldConfig->GetFieldName() == fieldName) {
            return fieldConfig;
        }
    }
    return nullptr;
}
std::vector<std::shared_ptr<FieldConfig>> FakeTabletSchema::GetFieldConfigs() const { return _fieldConfigs; }
std::vector<std::shared_ptr<FieldConfig>> FakeTabletSchema::GetIndexFieldConfigs(const std::string& indexType) const
{
    std::vector<std::shared_ptr<FieldConfig>> result;
    for (const auto& indexConfig : _indexConfigs) {
        if (indexConfig->GetIndexType() == indexType) {
            const auto& fieldConfigs = indexConfig->GetFieldConfigs();
            result.insert(result.end(), fieldConfigs.begin(), fieldConfigs.end());
        }
    }
    return result;
}
std::shared_ptr<IIndexConfig> FakeTabletSchema::GetIndexConfig(const std::string& indexType,
                                                               const std::string& indexName) const
{
    for (const auto& indexConfig : _indexConfigs) {
        if (indexConfig->GetIndexType() == indexType && indexConfig->GetIndexName() == indexName) {
            return indexConfig;
        }
    }
    return nullptr;
}
std::vector<std::shared_ptr<IIndexConfig>> FakeTabletSchema::GetIndexConfigs(const std::string& indexType) const
{
    std::vector<std::shared_ptr<IIndexConfig>> result;
    std::copy_if(_indexConfigs.begin(), _indexConfigs.end(), std::back_inserter(result),
                 [&indexType](const std::shared_ptr<IIndexConfig>& indexConfig) {
                     return indexConfig->GetIndexType() == indexType;
                 });
    return result;
}
std::vector<std::shared_ptr<IIndexConfig>> FakeTabletSchema::GetIndexConfigs() const { return _indexConfigs; }
std::vector<std::shared_ptr<IIndexConfig>>
FakeTabletSchema::GetIndexConfigsByFieldName(const std::string& fieldName) const
{
    std::vector<std::shared_ptr<IIndexConfig>> result;
    for (const auto& indexConfig : _indexConfigs) {
        const auto& fieldConfigs = indexConfig->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            if (fieldConfig->GetFieldName() == fieldName) {
                result.emplace_back(indexConfig);
                break;
            }
        }
    }
    return result;
}
std::shared_ptr<IIndexConfig> FakeTabletSchema::GetPrimaryKeyIndexConfig() const
{
    assert(false);
    return nullptr;
}
const MutableJson& FakeTabletSchema::GetRuntimeSettings() const { return _runtimeSettings; }
const indexlib::util::JsonMap& FakeTabletSchema::GetUserDefinedParam() const { return _userDefinedParam; }
bool FakeTabletSchema::Serialize(bool isCompact, std::string* output) const
{
    assert(false);
    return false;
}
const std::shared_ptr<indexlib::config::IndexPartitionSchema>& FakeTabletSchema::GetLegacySchema() const
{
    assert(false);
    static std::shared_ptr<indexlib::config::IndexPartitionSchema> legacySchema;
    return legacySchema;
}

std::vector<std::shared_ptr<FieldConfig>>& FakeTabletSchema::MutableFieldConfigs() { return _fieldConfigs; }
std::vector<std::shared_ptr<IIndexConfig>>& FakeTabletSchema::MutableIndexConfigs() { return _indexConfigs; }
indexlib::util::JsonMap& FakeTabletSchema::MutableSettings() { return _settings; }
indexlib::util::JsonMap& FakeTabletSchema::MutableUserDefinedParam() { return _userDefinedParam; }
MutableJson& FakeTabletSchema::MutableRuntimeSettings() { return _runtimeSettings; }
} // namespace indexlibv2::config
