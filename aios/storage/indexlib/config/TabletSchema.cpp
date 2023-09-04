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
#include "indexlib/config/TabletSchema.h"

#include "indexlib/config/UnresolvedSchema.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, TabletSchema);

TabletSchema::TabletSchema() : _impl(std::make_unique<UnresolvedSchema>()) {}
TabletSchema::~TabletSchema() {}

bool TabletSchema::LoadLegacySchema(const std::string& jsonStr,
                                    std::shared_ptr<indexlib::config::IndexPartitionSchema>& legacySchema)
{
    auto tabletSchema = std::make_shared<TabletSchema>();
    if (!tabletSchema->Deserialize(jsonStr)) {
        return false;
    }
    legacySchema = tabletSchema->GetLegacySchema();
    AUTIL_LOG(DEBUG, "load legacy schema success.");
    return true;
}

bool TabletSchema::IsLegacySchema(const autil::legacy::json::JsonMap& jsonMap)
{
    return UnresolvedSchema::IsLegacySchema(jsonMap);
}

const std::string& TabletSchema::GetTableType() const { return _impl->GetTableType(); }
const std::string& TabletSchema::GetTableName() const { return _impl->GetTableName(); }

schemaid_t TabletSchema::GetSchemaId() const { return _impl->GetSchemaId(); }
std::string TabletSchema::GetSchemaFileName() const { return _impl->GetSchemaFileName(); }
std::string TabletSchema::GetSchemaFileName(schemaid_t schemaId)
{
    return UnresolvedSchema::GetSchemaFileName(schemaId);
}

bool TabletSchema::Serialize(bool isCompact, std::string* output) const { return _impl->Serialize(isCompact, output); }
bool TabletSchema::Deserialize(const std::string& jsonStr) { return _impl->Deserialize(jsonStr); }

size_t TabletSchema::GetFieldCount() const { return _impl->GetFieldCount(); }

fieldid_t TabletSchema::GetFieldId(const std::string& fieldName) const { return _impl->GetFieldId(fieldName); }

std::shared_ptr<FieldConfig> TabletSchema::GetFieldConfig(const std::string& fieldName) const
{
    return _impl->GetFieldConfig(fieldName);
}

std::shared_ptr<FieldConfig> TabletSchema::GetFieldConfig(fieldid_t fieldId) const
{
    return _impl->GetFieldConfig(fieldId);
}

std::vector<std::shared_ptr<FieldConfig>> TabletSchema::GetFieldConfigs() const { return _impl->GetFieldConfigs(); }

std::vector<std::shared_ptr<FieldConfig>> TabletSchema::GetIndexFieldConfigs(const std::string& indexType) const
{
    return _impl->GetIndexFieldConfigs(indexType);
}

std::vector<std::shared_ptr<IIndexConfig>> TabletSchema::GetIndexConfigs() const { return _impl->GetIndexConfigs(); }
std::vector<std::shared_ptr<IIndexConfig>> TabletSchema::GetIndexConfigs(const std::string& indexType) const
{
    return _impl->GetIndexConfigs(indexType);
}
std::vector<std::shared_ptr<IIndexConfig>> TabletSchema::GetIndexConfigsByFieldName(const std::string& fieldName) const
{
    return _impl->GetIndexConfigsByFieldName(fieldName);
}
std::shared_ptr<IIndexConfig> TabletSchema::GetIndexConfig(const std::string& indexType,
                                                           const std::string& indexName) const
{
    return _impl->GetIndexConfig(indexType, indexName);
}

const std::shared_ptr<indexlib::config::IndexPartitionSchema>& TabletSchema::GetLegacySchema() const
{
    return _impl->GetLegacySchema();
}

void TabletSchema::SetSchemaId(schemaid_t schemaId) { _impl->SetSchemaId(schemaId); }

UnresolvedSchema* TabletSchema::TEST_GetImpl() const { return _impl.get(); }

void TabletSchema::TEST_SetTableName(const std::string& name) { _impl->TEST_SetTableName(name); }

std::pair<bool, const autil::legacy::Any&> TabletSchema::GetRuntimeSetting(const std::string& key) const
{
    return _impl->GetRuntimeSetting(key);
}

const indexlib::util::JsonMap& TabletSchema::GetUserDefinedParam() const { return _impl->GetUserDefinedParam(); }
const MutableJson& TabletSchema::GetRuntimeSettings() const { return _impl->GetRuntimeSettings(); }

std::shared_ptr<IIndexConfig> TabletSchema::GetPrimaryKeyIndexConfig() const
{
    return _impl->GetPrimaryKeyIndexConfig();
}

} // namespace indexlibv2::config
