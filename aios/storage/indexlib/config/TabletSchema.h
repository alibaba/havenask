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
#pragma once
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/util/JsonMap.h"

namespace indexlibv2::config {
class UnresolvedSchema;

// REFERENCE: TabletSchema.txt
class TabletSchema : public ITabletSchema
{
public:
    static bool LoadLegacySchema(const std::string& jsonStr,
                                 std::shared_ptr<indexlib::config::IndexPartitionSchema>& legacySchema);
    static bool IsLegacySchema(const autil::legacy::json::JsonMap& jsonMap);
    static std::string GetSchemaFileName(schemaid_t schemaId);
    static Status CheckUpdateSchema(const std::shared_ptr<ITabletSchema>& oldSchema,
                                    const std::shared_ptr<ITabletSchema>& newSchema);
    // use framework::TabletSchemaLoader load new shcema
public:
    TabletSchema();
    ~TabletSchema() override;

public:
    // base
    const std::string& GetTableType() const override;
    const std::string& GetTableName() const override;
    schemaid_t GetSchemaId() const override;
    std::string GetSchemaFileName() const override;
    // fields
    size_t GetFieldCount() const override;
    fieldid_t GetFieldId(const std::string& fieldName) const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(fieldid_t fieldId) const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(const std::string& fieldName) const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetIndexFieldConfigs(const std::string& indexType) const override;
    // indexes
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& indexType,
                                                 const std::string& indexName) const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs(const std::string& indexType) const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs() const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigsByFieldName(const std::string& fieldName) const override;
    std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const override;
    // runtime settings
    const MutableJson& GetRuntimeSettings() const override;
    // user defined param, the payload no affect on index, we just serialize and deserialize it
    const indexlib::util::JsonMap& GetUserDefinedParam() const override;
    // serialization
    bool Serialize(bool isCompact, std::string* output) const override;
    // legacy
    const std::shared_ptr<indexlib::config::IndexPartitionSchema>& GetLegacySchema() const override;

public:
    void SetSchemaId(schemaid_t schemaId);
    template <typename T>
    std::pair<Status, T> GetRuntimeSetting(const std::string& key) const;

public:
    UnresolvedSchema* TEST_GetImpl() const;
    void TEST_SetTableName(const std::string& name);

private:
    virtual bool Deserialize(const std::string& jsonStr);

private:
    std::pair<bool, const autil::legacy::Any&> GetRuntimeSetting(const std::string& key) const;

private:
    std::unique_ptr<UnresolvedSchema> _impl;

private:
    friend class SchemaResolver;
    friend class TabletSchemaDelegation;
    friend class LegacySchemaConvertor;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
std::pair<Status, T> TabletSchema::GetRuntimeSetting(const std::string& key) const
{
    T setting {};
    auto [isExist, settingAny] = GetRuntimeSetting(key);
    if (!isExist) {
        return {Status::NotFound(), setting};
    }
    try {
        autil::legacy::FromJson(setting, settingAny);
    } catch (const autil::legacy::ExceptionBase& e) {
        Status status = Status::ConfigError("[%s] is json invalid, %s", key.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, setting};
    }
    return {Status::OK(), setting};
}

} // namespace indexlibv2::config
