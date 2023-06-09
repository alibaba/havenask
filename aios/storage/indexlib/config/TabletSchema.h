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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"

namespace indexlib::config {
class IndexPartitionSchema;
} // namespace indexlib::config

namespace indexlibv2::config {
class IIndexConfig;
class UnresolvedSchema;
class FieldConfig;

// REFERENCE: TabletSchema.txt
class TabletSchema : public ITabletSchema
{
public:
    static bool LoadLegacySchema(const std::string& jsonStr,
                                 std::shared_ptr<indexlib::config::IndexPartitionSchema>& legacySchema);
    static bool IsLegacySchema(const autil::legacy::json::JsonMap& jsonMap);

public:
    TabletSchema();
    ~TabletSchema() override;

    // ITabletSchema
    const std::string& GetTableType() const override;
    const std::string& GetTableName() const override;
    size_t GetFieldCount() const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(const std::string& fieldName) const override;
    fieldid_t GetFieldId(const std::string& fieldName) const override;
    std::shared_ptr<FieldConfig> GetFieldConfig(fieldid_t fieldId) const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetIndexFieldConfigs(const std::string& indexType) const override;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs(const std::string& indexType) const override;
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& indexType,
                                                 const std::string& indexName) const override;
    std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const override;
    bool GetValueFromUserDefinedParam(const std::string& key, std::string& value) const override;
    void SetUserDefinedParam(const std::string& key, const std::string& value) override;
    autil::legacy::json::JsonMap GetUserDefinedParam() const override;
    bool Serialize(bool isCompact, std::string* output) const override;
    const std::shared_ptr<indexlib::config::IndexPartitionSchema>& GetLegacySchema() const override;

public:
    virtual schemaid_t GetSchemaId() const;
    void SetSchemaId(schemaid_t schemaId);
    virtual std::string GetSchemaFileName() const;

    virtual std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs() const;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigsByFieldName(const std::string& fieldName) const;

    template <typename T>
    std::pair<Status, T> GetSetting(const std::string& key) const;

    template <typename T>
    std::pair<Status, T> GetRuntimeSetting(const std::string& path) const;
    template <typename T>
    bool SetRuntimeSetting(const std::string& path, const T& value);

public:
    static std::string GetSchemaFileName(schemaid_t schemaId);

public:
    UnresolvedSchema* TEST_GetImpl() const;
    void TEST_SetTableName(const std::string& name);

private:
    virtual bool Deserialize(const std::string& jsonStr);

private:
    std::pair<bool, const autil::legacy::Any&> GetSettingConfig(const std::string& key) const;
    std::pair<bool, autil::legacy::Any> GetRuntimeSettingObject(const std::string& path) const;
    bool SetRuntimeSettingObject(const std::string& path, const autil::legacy::Any& any);

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
std::pair<Status, T> TabletSchema::GetSetting(const std::string& key) const
{
    T setting {};
    auto [isExist, settingAny] = GetSettingConfig(key);
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

template <typename T>
std::pair<Status, T> TabletSchema::GetRuntimeSetting(const std::string& path) const
{
    T setting {};
    auto [isExist, settingAny] = GetRuntimeSettingObject(path);
    if (!isExist) {
        return {Status::NotFound(), T()};
    }
    try {
        autil::legacy::FromJson(setting, settingAny);
    } catch (const autil::legacy::ExceptionBase& e) {
        Status status = Status::ConfigError("decode jsonPath[%s] failed: %s", path.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, T()};
    }
    return {Status::OK(), setting};
}

template <typename T>
bool TabletSchema::SetRuntimeSetting(const std::string& path, const T& value)
{
    try {
        auto any = autil::legacy::ToJson(value);
        return SetRuntimeSettingObject(path, any);
    } catch (const autil::legacy::ExceptionBase& e) {
        Status status = Status::ConfigError("encode jsonPath[%s] failed: %s", path.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return false;
    }
}

} // namespace indexlibv2::config
