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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/util/JsonMap.h"

namespace indexlibv2::index {
class IIndexFactory;
}

namespace indexlib::config {
class IndexPartitionSchema;
} // namespace indexlib::config

namespace indexlibv2::config {
class IIndexConfig;

class UnresolvedSchema
{
public:
    UnresolvedSchema();
    ~UnresolvedSchema();

public:
    // base
    const std::string& GetTableType() const { return _tableType; }
    const std::string& GetTableName() const { return _tableName; }
    schemaid_t GetSchemaId() const { return _schemaId; }
    std::string GetSchemaFileName() const;

    bool Serialize(bool isCompact, std::string* output);
    bool Deserialize(const std::string& jsonStr);

    // fields
    size_t GetFieldCount() const;
    fieldid_t GetFieldId(const std::string& fieldName) const;
    std::shared_ptr<FieldConfig> GetFieldConfig(fieldid_t fieldId) const;
    std::shared_ptr<FieldConfig> GetFieldConfig(const std::string& fieldName) const;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const;
    std::vector<std::shared_ptr<FieldConfig>> GetIndexFieldConfigs(const std::string& indexType) const;
    Status AddFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig);

    // indexes
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs() const;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigs(const std::string& indexType) const;
    std::vector<std::shared_ptr<IIndexConfig>> GetIndexConfigsByFieldName(const std::string& fieldName) const;
    std::shared_ptr<IIndexConfig> GetIndexConfig(const std::string& indexType, const std::string& indexName) const;
    std::shared_ptr<IIndexConfig> GetPrimaryKeyIndexConfig() const;
    void SetPrimaryKeyIndexConfig(const std::shared_ptr<IIndexConfig>& pkConfig);
    Status AddIndexConfig(const std::shared_ptr<IIndexConfig>& indexConfig);
    Status AddIndexConfig(const std::string& indexType, const std::shared_ptr<IIndexConfig>& indexConfig);

    // runtime setting
    template <typename T>
    bool SetRuntimeSetting(const std::string& path, const T& value, bool overwrite);
    template <typename T>
    Status GetRuntimeSetting(const std::string& path, T& value, const T& defaultValue) const;
    const MutableJson& GetRuntimeSettings() const { return _runtimeSettings; }
    std::pair<bool, const autil::legacy::Any&> GetRuntimeSetting(const std::string& key) const;
    bool SetRuntimeSetting(const std::string& key, const autil::legacy::Any& value, bool overwrite);

    // user payload
    const indexlib::util::JsonMap& GetUserDefinedParam() const;

public:
    void SetSchemaId(schemaid_t schemaId);
    static std::string GetSchemaFileName(schemaid_t schemaId);
    static bool IsLegacySchema(const autil::legacy::json::JsonMap& jsonMap);

public:
    void TEST_SetTableName(const std::string& name);
    void TEST_SetTableType(const std::string& type);
    template <typename T>
    bool TEST_SetSetting(const std::string& path, const T& value, bool overwrite);

private:
    const std::shared_ptr<indexlib::config::IndexPartitionSchema>& GetLegacySchema() const { return _legacySchema; }
    template <typename T>
    bool GetJsonValue(const autil::legacy::json::JsonMap& parent, const std::string& key, T* value) const;
    Status PrepareIndexConfigs(const autil::legacy::json::JsonMap& jsonMap);

    void SerializeIndexes(autil::legacy::json::JsonMap& indexes) const;
    // serialize/deserialize infos all index shared, such as table_name table_type fields
    bool JsonizeCommonInfo(autil::legacy::Jsonizable::JsonWrapper& json);
    Status LoadIndexConfigs(const index::IIndexFactory* indexFactory, const autil::legacy::json::JsonArray& jsonArray,
                            const IndexConfigDeserializeResource& resource,
                            std::vector<std::shared_ptr<IIndexConfig>>* indexConfigs) const;
    Status DoAddIndexConfig(const std::string& indexType, const std::shared_ptr<IIndexConfig>& indexConfig,
                            bool nativeFromJson);

    template <typename Func>
    void ForEachIndexConfigs(const std::string& indexType, Func fn) const;
    template <typename Func>
    void ForEachIndexConfigs(Func fn) const;

private:
    static constexpr const char TABLET_SCHEMA_FORMAT_VERSION[] = "format_version";
    static constexpr const char TABLET_SCHEMA_TABLE_NAME[] = "table_name";
    static constexpr const char TABLET_SCHEMA_TABLE_TYPE[] = "table_type";
    static constexpr const char TABLET_SCHEMA_FIELDS[] = "fields";
    static constexpr const char TABLET_SCHEMA_INDEXES[] = "indexes";
    static constexpr const char TABLET_SCHEMA_SETTINGS[] = "settings";
    static constexpr const char TABLET_SCHEMA_USER_DEFINED_PARAM[] = "user_defined_param";
    static constexpr const char TABLET_SCHEMA_ID[] = "versionid";

private:
    std::string _tableType;
    std::string _tableName;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;
    int32_t _formatVersion = 2;

    std::shared_ptr<indexlib::config::IndexPartitionSchema> _legacySchema;

    std::vector<std::shared_ptr<FieldConfig>> _fields;
    std::unordered_map<std::string, fieldid_t> _fieldNameToIdMap;

    std::vector<std::shared_ptr<IIndexConfig>> _nativeIndexConfigs;
    std::vector<std::pair</*indexType*/ std::string, std::shared_ptr<IIndexConfig>>> _extendedIndexConfigs;
    std::shared_ptr<IIndexConfig> _primaryKeyIndexConfig;
    std::map<std::pair</*name*/ std::string, /*type*/ std::string>, std::shared_ptr<IIndexConfig>> _indexConfigMap;

    autil::legacy::json::JsonMap _settings; // load from schema file, cannot be modified, will be serialized
    MutableJson _runtimeSettings; // copy from _settings when deserialize, can be modified, will not be serialized
    indexlib::util::JsonMap _userDefinedParam;

private:
    friend class TabletSchema;
    friend class LegacySchemaConvertor;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
inline Status UnresolvedSchema::GetRuntimeSetting(const std::string& key, T& value, const T& defaultValue) const
{
    auto [status, val] = _runtimeSettings.GetValue<T>(key);
    if (status.IsOK()) {
        value = val;
        return Status::OK();
    } else if (status.IsNotFound()) {
        value = defaultValue;
        return Status::OK();
    } else {
        return status;
    }
}

template <typename T>
inline bool UnresolvedSchema::SetRuntimeSetting(const std::string& key, const T& value, bool overwrite)
{
    try {
        return SetRuntimeSetting(key, autil::legacy::ToJson(value), overwrite);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "[%s] to json failed, %s", key.c_str(), e.what());
        return false;
    }
}

template <typename T>
inline bool UnresolvedSchema::TEST_SetSetting(const std::string& key, const T& value, bool overwrite)
{
    try {
        const autil::legacy::Any& any = autil::legacy::ToJson(value);
        auto iter = _settings.find(key);
        if (iter != _settings.end() && !overwrite) {
            AUTIL_LOG(ERROR, "schema setting key [%s] is exist, value is [%s], cannot set again", key.c_str(),
                      autil::legacy::ToJsonString(iter->second, true).c_str());

            return false;
        }
        _settings[key] = any;
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "[%s] to json failed, %s", key.c_str(), e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "[%s] to json failed, unknown exception", key.c_str());
        return false;
    }
    return SetRuntimeSetting(key, value, overwrite);
}

} // namespace indexlibv2::config
