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
#include "indexlib/config/UnresolvedSchema.h"

#include "UnresolvedSchema.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/LegacySchemaConvertor.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, UnresolvedSchema);

UnresolvedSchema::UnresolvedSchema() : _tableType() {}
UnresolvedSchema::~UnresolvedSchema() {}

std::string UnresolvedSchema::GetSchemaFileName() const { return GetSchemaFileName(_schemaId); }

void UnresolvedSchema::TEST_SetTableName(const std::string& name) { _tableName = name; }

void UnresolvedSchema::TEST_SetTableType(const std::string& type) { _tableType = type; }

std::string UnresolvedSchema::GetSchemaFileName(schemaid_t schemaId)
{
    if (schemaId == DEFAULT_SCHEMAID) {
        return SCHEMA_FILE_NAME;
    }
    return SCHEMA_FILE_NAME + "." + autil::StringUtil::toString(schemaId);
}

std::pair<bool, const autil::legacy::Any&> UnresolvedSchema::GetSettingConfig(const std::string& key) const
{
    static autil::legacy::Any empty;
    auto iter = _settings.find(key);
    if (iter != _settings.end()) {
        return {true, iter->second};
    }
    return {false, empty};
}

fieldid_t UnresolvedSchema::GetFieldId(const std::string& fieldName) const
{
    auto iter = _fieldNameToIdMap.find(fieldName);
    if (iter == _fieldNameToIdMap.end()) {
        return INVALID_FIELDID;
    }
    return iter->second;
}

std::shared_ptr<FieldConfig> UnresolvedSchema::GetFieldConfig(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= static_cast<fieldid_t>(_fields.size())) {
        return nullptr;
    }
    return _fields[fieldId];
}

std::shared_ptr<FieldConfig> UnresolvedSchema::GetFieldConfig(const std::string& fieldName) const
{
    return GetFieldConfig(GetFieldId(fieldName));
}

std::vector<std::shared_ptr<FieldConfig>> UnresolvedSchema::GetFieldConfigs() const { return _fields; }

size_t UnresolvedSchema::GetFieldCount() const { return _fields.size(); }

std::vector<std::shared_ptr<FieldConfig>> UnresolvedSchema::GetIndexFieldConfigs(const std::string& indexType) const
{
    if (indexType == GENERALIZED_VALUE_INDEX_TYPE_STR) {
        return _generalizedValueIndexFieldConfigs;
    } else {
        std::vector<std::shared_ptr<FieldConfig>> ret;
        std::map<fieldid_t, std::shared_ptr<FieldConfig>> fields;

        ForEachIndexConfigs(indexType, [&fields](const auto& indexConfig) {
            auto indexFields = indexConfig->GetFieldConfigs();
            for (const auto& fieldConfig : indexFields) {
                fields.insert({fieldConfig->GetFieldId(), fieldConfig});
            }
        });

        for (const auto& [fieldId, fieldConfig] : fields) {
            ret.push_back(fieldConfig);
        }
        return ret;
    }
}

bool UnresolvedSchema::SetSettingConfig(const std::string& key, const autil::legacy::Any& value, bool overwrite)
{
    auto iter = _settings.find(key);
    if (iter != _settings.end() && !overwrite) {
        AUTIL_LOG(ERROR, "schema setting key [%s] is exist, value is [%s], cannot set again", key.c_str(),
                  autil::legacy::ToJsonString(iter->second, true).c_str());
        return false;
    }
    _settings[key] = value;
    return true;
}

void UnresolvedSchema::SerializeIndexes(autil::legacy::json::JsonMap& indexes) const
{
    std::map<std::string, std::vector<autil::legacy::Any>> anyConfigs;
    for (const auto& indexConfig : _nativeIndexConfigs) {
        autil::legacy::Jsonizable::JsonWrapper jsonWrapper;
        indexConfig->Serialize(jsonWrapper);
        auto any = jsonWrapper.GetMap();
        auto indexType = indexConfig->GetIndexType();
        auto iter = anyConfigs.find(indexType);
        if (iter == anyConfigs.end()) {
            iter = anyConfigs.insert(anyConfigs.end(), {indexType, std::vector<autil::legacy::Any>()});
        }
        iter->second.push_back(any);
    }
    for (const auto& [k, v] : anyConfigs) {
        indexes[k] = autil::legacy::ToJson(v);
    }
}

bool UnresolvedSchema::Serialize(bool isCompact, std::string* output)
{
    autil::legacy::Jsonizable::JsonWrapper wrapper;
    autil::legacy::json::JsonMap jsonMap;
    if (_legacySchema) {
        jsonMap = LegacySchemaConvertor::ToJson(this);
    } else {
        if (!JsonizeCommonInfo(wrapper)) {
            AUTIL_LOG(ERROR, "schema [%s] serialize failed", _tableName.c_str());
            return false;
        }
        jsonMap = wrapper.GetMap();
    }
    autil::legacy::json::JsonMap indexes;
    // TODO(yonghao.fyh) : select certain index by TableType
    SerializeIndexes(indexes);

    if (!indexes.empty()) {
        jsonMap[TABLET_SCHEMA_INDEXES] = autil::legacy::ToJson(indexes);
    }
    if (!_settings.empty()) {
        jsonMap[TABLET_SCHEMA_SETTINGS] = autil::legacy::ToJson(_settings);
    }
    *output = autil::legacy::ToJsonString(jsonMap, isCompact);
    return true;
}

bool UnresolvedSchema::Deserialize(const std::string& jsonStr)
{
    try {
        autil::legacy::Any any = autil::legacy::json::ParseJson(jsonStr);
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
        autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
        if (IsLegacySchema(jsonMap)) {
            auto status = LegacySchemaConvertor::FromJson(wrapper, this);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "schema deserialize failed, [%s]", status.ToString().c_str());
                return false;
            }
        } else {
            if (!JsonizeCommonInfo(wrapper)) {
                AUTIL_LOG(ERROR, "schema deserialize failed, [%s]", jsonStr.c_str());
                return false;
            }
        }
        auto status = PrepareIndexConfigs(jsonMap);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "tablet schema init failed, prepare index config fail [%s]", jsonStr.c_str());
            return false;
        }
        return true;
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "tablet schema init failed, name[%s], type[%s], exception[%s]", _tableName.c_str(),
                  _tableType.c_str(), e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "tablet schema init failed, name[%s], type[%s], unknown exception", _tableName.c_str(),
                  _tableType.c_str());
    }
    return false;
}

bool UnresolvedSchema::JsonizeCommonInfo(autil::legacy::Jsonizable::JsonWrapper& json)
{
    const int32_t DEFAULT_FORMAT_VERSION = 1;
    json.Jsonize(TABLET_SCHEMA_TABLE_NAME, _tableName, "");
    if (_tableName.empty()) {
        AUTIL_LOG(ERROR, "'table_name' must be specified, and not empty");
        return false;
    }
    json.Jsonize(TABLET_SCHEMA_TABLE_TYPE, _tableType, "normal");
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        if (_schemaId != DEFAULT_SCHEMAID) {
            json.Jsonize(TABLET_SCHEMA_ID, _schemaId);
        }
        if (_formatVersion > DEFAULT_FORMAT_VERSION) {
            json.Jsonize(TABLET_SCHEMA_FORMAT_VERSION, _formatVersion);
        }
        std::vector<autil::legacy::Any> anyVec;
        anyVec.reserve(_fields.size());
        for (const auto& fieldConfig : _fields) {
            anyVec.push_back(autil::legacy::ToJson(*fieldConfig));
        }
        json.Jsonize(TABLET_SCHEMA_FIELDS, anyVec);
    } else {
        json.Jsonize(TABLET_SCHEMA_ID, _schemaId, _schemaId);
        json.Jsonize(TABLET_SCHEMA_FORMAT_VERSION, _formatVersion, DEFAULT_FORMAT_VERSION);
        autil::legacy::json::JsonArray fields;
        auto jsonMap = json.GetMap();
        _fields.clear();
        _fieldNameToIdMap.clear();
        if (GetJsonValue(jsonMap, TABLET_SCHEMA_FIELDS, &fields)) {
            for (const auto& fieldJson : fields) {
                autil::legacy::Jsonizable::JsonWrapper wrapper(fieldJson);
                auto fieldConfig = std::make_shared<FieldConfig>();
                fieldConfig->Jsonize(wrapper);
                auto status = AddFieldConfig(fieldConfig);
                if (!status.IsOK()) {
                    AUTIL_LOG(ERROR, "add field config [%s] failed", fieldConfig->GetFieldName().c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

Status UnresolvedSchema::PrepareIndexConfigs(const autil::legacy::json::JsonMap& jsonMap)
{
    _nativeIndexConfigs.clear();
    _extendedIndexConfigs.clear();
    _indexConfigMap.clear();
    GetJsonValue(jsonMap, TABLET_SCHEMA_SETTINGS, &_settings);

    autil::legacy::json::JsonMap indexes;
    GetJsonValue(jsonMap, TABLET_SCHEMA_INDEXES, &indexes);
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    IndexConfigDeserializeResource resource(_fields, _settings, _runtimeSettings);
    for (const auto& [key, value] : indexes) {
        std::string keyStr = autil::legacy::AnyCast<std::string>(key);
        auto [factoryStatus, indexFactory] = indexFactoryCreator->Create(keyStr);
        if (!factoryStatus.IsOK()) {
            AUTIL_LOG(ERROR, "create index factory for index type [%s] failed, [%s]", keyStr.c_str(),
                      factoryStatus.ToString().c_str());
            return factoryStatus;
        }
        std::vector<std::shared_ptr<IIndexConfig>> indexConfigs;
        auto status = LoadIndexConfigs(indexFactory, autil::legacy::AnyCast<autil::legacy::json::JsonArray>(value),
                                       resource, &indexConfigs);
        RETURN_IF_STATUS_ERROR(status, "[%s] index config load failed.", keyStr.c_str());

        for (const auto& indexConfig : indexConfigs) {
            auto status = DoAddIndexConfig(indexConfig->GetIndexType(), indexConfig, /*nativeFromJson*/ true);
            RETURN_IF_STATUS_ERROR(status, "add index config [%s][%s] failed", indexConfig->GetIndexType().c_str(),
                                   indexConfig->GetIndexName().c_str());
        }
    }
    if (_nativeIndexConfigs.empty()) {
        AUTIL_LOG(INFO, "tablet schema index configs is empty. tableType[%s] tableName[%s]", GetTableType().c_str(),
                  GetTableName().c_str());
    }
    return Status::OK();
}

Status UnresolvedSchema::LoadIndexConfigs(const index::IIndexFactory* indexFactory,
                                          const autil::legacy::json::JsonArray& jsonArray,
                                          const IndexConfigDeserializeResource& resource,
                                          std::vector<std::shared_ptr<IIndexConfig>>* indexConfigs) const
{
    assert(indexFactory);
    indexConfigs->clear();
    indexConfigs->reserve(jsonArray.size());

    size_t i = 0;
    for (const auto& json : jsonArray) {
        try {
            // may throw exception
            std::shared_ptr<IIndexConfig> config = indexFactory->CreateIndexConfig(json);
            if (!config) {
                RETURN_IF_STATUS_ERROR(Status::ConfigError(), "create index config failed");
            }
            // may throw exception
            config->Deserialize(json, i, resource);
            config->Check();
            indexConfigs->push_back(config);
        } catch (const std::exception& e) {
            RETURN_IF_STATUS_ERROR(Status::ConfigError(), "deserialize index config failed, json[%s] exception[%s]",
                                   autil::legacy::ToJsonString(json, true).c_str(), e.what());
        }
        ++i;
    }
    return Status::OK();
}

std::vector<std::shared_ptr<IIndexConfig>> UnresolvedSchema::GetIndexConfigs() const
{
    std::vector<std::shared_ptr<IIndexConfig>> ret;
    ForEachIndexConfigs([&ret](const auto& indexConfig) { ret.push_back(indexConfig); });
    return ret;
}

std::vector<std::shared_ptr<IIndexConfig>> UnresolvedSchema::GetIndexConfigs(const std::string& indexType) const
{
    std::vector<std::shared_ptr<IIndexConfig>> ret;
    ForEachIndexConfigs(indexType, [&ret](const auto& indexConfig) { ret.push_back(indexConfig); });
    return ret;
}

std::vector<std::shared_ptr<IIndexConfig>>
UnresolvedSchema::GetIndexConfigsByFieldName(const std::string& fieldName) const
{
    std::vector<std::shared_ptr<IIndexConfig>> ret;
    ForEachIndexConfigs([&ret, &fieldName](const auto& indexConfig) {
        auto fields = indexConfig->GetFieldConfigs();
        for (const auto& fieldConfig : fields) {
            if (fieldConfig->GetFieldName() == fieldName) {
                ret.push_back(indexConfig);
                break;
            }
        }
    });

    return ret;
}

std::shared_ptr<IIndexConfig> UnresolvedSchema::GetIndexConfig(const std::string& indexType,
                                                               const std::string& indexName) const
{
    // if (indexType == "inverted_index") {
    //     std::string originalIndexName;
    //     if (InvertedIndexConfig::GetIndexNameFromShardingIndexName(indexName, originalIndexName)) {
    //         assert(false);
    //     }
    // }
    auto iter = _indexConfigMap.find({indexName, indexType});
    if (iter == _indexConfigMap.end()) {
        static const std::shared_ptr<IIndexConfig> NULL_INDEXCONFIG;
        return NULL_INDEXCONFIG;
    }
    return iter->second;
}

std::shared_ptr<IIndexConfig> UnresolvedSchema::GetPrimaryKeyIndexConfig() const { return _primaryKeyIndexConfig; }

void UnresolvedSchema::SetPrimaryKeyIndexConfig(const std::shared_ptr<IIndexConfig>& pkConfig)
{
    _primaryKeyIndexConfig = pkConfig;
}

void UnresolvedSchema::SetGeneralizedValueIndexFieldConfigs(std::vector<std::shared_ptr<FieldConfig>> fieldConfigs)
{
    _generalizedValueIndexFieldConfigs = std::move(fieldConfigs);
}

Status UnresolvedSchema::AddIndexConfig(const std::shared_ptr<IIndexConfig>& indexConfig)
{
    return DoAddIndexConfig(indexConfig->GetIndexType(), indexConfig, /*nativeFromJson*/ false);
}

Status UnresolvedSchema::AddIndexConfig(const std::string& indexType, const std::shared_ptr<IIndexConfig>& indexConfig)
{
    return DoAddIndexConfig(indexType, indexConfig, /*nativeFromJson*/ false);
}

Status UnresolvedSchema::DoAddIndexConfig(const std::string& indexType,
                                          const std::shared_ptr<IIndexConfig>& indexConfig, bool nativeFromJson)
{
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "config is nullptr");
        return Status::InvalidArgs("config is nullptr");
    }
    if (GetIndexConfig(indexType, indexConfig->GetIndexName())) {
        AUTIL_LOG(ERROR, "index config [%s] [%s] already exist", indexType.c_str(),
                  indexConfig->GetIndexName().c_str());
        return Status::Corruption("index config already exist");
    }
    if (nativeFromJson) {
        _nativeIndexConfigs.push_back(indexConfig);
    } else {
        _extendedIndexConfigs.push_back({indexType, indexConfig});
    }
    _indexConfigMap[{indexConfig->GetIndexName(), indexType}] = indexConfig;

    return Status::OK();
}

template <typename T>
bool UnresolvedSchema::GetJsonValue(const autil::legacy::json::JsonMap& parent, const std::string& key, T* value) const
{
    auto iter = parent.find(key);
    if (iter != parent.end()) {
        *value = autil::legacy::AnyCast<T>(iter->second);
        return true;
    }
    return false;
}

bool UnresolvedSchema::GetValueFromUserDefinedParam(const std::string& key, std::string& value) const
{
    auto [status, settingValue] = GetSetting<std::string>(key);
    if (!status.IsOK()) {
        return false;
    }
    value = settingValue;
    return true;
}

void UnresolvedSchema::SetUserDefinedParam(const std::string& key, const std::string& value)
{
    [[maybe_unused]] auto r = SetSettingConfig(key, value, /*overwrite*/ true);
    assert(r);
}

Status UnresolvedSchema::AddFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    assert(fieldConfig);
    auto iter = _fieldNameToIdMap.find(fieldConfig->GetFieldName());
    if (iter != _fieldNameToIdMap.end()) {
        RETURN_STATUS_ERROR(ConfigError, "duplicate field name[%s]", fieldConfig->GetFieldName().c_str());
    }
    fieldid_t fieldId = _fieldNameToIdMap.size();
    assert(fieldConfig->GetFieldId() == INVALID_FIELDID || fieldConfig->GetFieldId() == fieldId);
    fieldConfig->SetFieldId(fieldId);
    _fields.push_back(fieldConfig);
    _fieldNameToIdMap[fieldConfig->GetFieldName()] = fieldId;
    return Status::OK();
}

bool UnresolvedSchema::IsLegacySchema(const autil::legacy::json::JsonMap& jsonMap)
{
    if (jsonMap.find("regions") != jsonMap.end() || jsonMap.find("indexs") != jsonMap.end() ||
        jsonMap.find("attributes") != jsonMap.end() || jsonMap.find("summarys") != jsonMap.end() ||
        jsonMap.find("customized_table_config") != jsonMap.end()) {
        return true;
    }
    return false;
}

void UnresolvedSchema::SetSchemaId(schemaid_t schemaId) { _schemaId = schemaId; }

template <typename Func>
void UnresolvedSchema::ForEachIndexConfigs(const std::string& indexType, Func fn) const
{
    for (const auto& indexConfig : _nativeIndexConfigs) {
        if (indexType == indexConfig->GetIndexType()) {
            fn(indexConfig);
        }
    }

    for (const auto& [type, indexConfig] : _extendedIndexConfigs) {
        if (type == indexType) {
            fn(indexConfig);
        }
    }
}

template <typename Func>
void UnresolvedSchema::ForEachIndexConfigs(Func fn) const
{
    for (const auto& indexConfig : _nativeIndexConfigs) {
        fn(indexConfig);
    }

    for (const auto& [type, indexConfig] : _extendedIndexConfigs) {
        if (type == indexConfig->GetIndexType()) {
            fn(indexConfig);
        }
    }
}

} // namespace indexlibv2::config
