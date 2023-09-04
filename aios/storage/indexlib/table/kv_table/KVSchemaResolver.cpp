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
#include "indexlib/table/kv_table/KVSchemaResolver.h"

#include <assert.h>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/TTLSettings.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVSchemaResolver);

KVSchemaResolver::KVSchemaResolver() {}
KVSchemaResolver::~KVSchemaResolver() {}

Status KVSchemaResolver::Check(const config::TabletSchema& schema) const
{
    auto indexConfigs = schema.GetIndexConfigs(index::KV_INDEX_TYPE_STR);
    if (indexConfigs.empty()) {
        return Status::ConfigError("no valid kv index config");
    }
    for (const auto& indexConfig : indexConfigs) {
        try {
            indexConfig->Check();
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "check kvIndexConfig [%s] failed exception [%s].", indexConfig->GetIndexName().c_str(),
                      e.what());
            return Status::ConfigError("check kv index config failed");
        }
    }
    return Status::OK();
}

void KVSchemaResolver::FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                         config::UnresolvedSchema* schema)
{
    if (!legacySchema.TTLEnabled()) {
        return;
    }
    auto [status, ttlEnable] = schema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    auto [status1, fieldName] = schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
    auto [status2, defaultTTL] = schema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
    auto [status3, ttlFromDoc] = schema->GetRuntimeSettings().GetValue<bool>("ttl_from_doc");
    if (status.IsOK() || status1.IsOK() || status2.IsOK() || status3.IsOK()) {
        // tablet schema setting has ttl config, not use legacy schema
        return;
    }

    bool ret = schema->SetRuntimeSetting("enable_ttl", true, false);
    ret = schema->SetRuntimeSetting("ttl_field_name", legacySchema.GetTTLFieldName(), false) && ret;
    ret = schema->SetRuntimeSetting("default_ttl", legacySchema.GetDefaultTTL(), false) && ret;
    ret = schema->SetRuntimeSetting("ttl_from_doc", legacySchema.TTLFromDoc(), false) && ret;
    assert(ret);
}

Status KVSchemaResolver::FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                                          config::UnresolvedSchema* schema)
{
    const std::string& indexType = index::KV_INDEX_TYPE_STR;
    assert(schema);
    auto indexConfigs = schema->GetIndexConfigs(indexType);
    if (!indexConfigs.empty()) {
        return Status::OK();
    }
    // 这一坨东西主要是用来兼容老版本schema格式
    const auto& indexSchema = legacySchema.GetIndexSchema();
    if (indexSchema != nullptr && indexSchema->HasPrimaryKeyIndex()) {
        auto pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        auto legacyConfig = std::dynamic_pointer_cast<indexlib::config::KVIndexConfig>(pkConfig);
        assert(legacyConfig);
        auto kvIndexConfig = std::shared_ptr<indexlibv2::config::KVIndexConfig>(legacyConfig->MakeKVIndexConfigV2());
        assert(kvIndexConfig);
        auto valueConfig = kvIndexConfig->GetValueConfig();
        assert(valueConfig);
        valueConfig->EnableCompactFormat(true);
        auto status = schema->AddIndexConfig(kvIndexConfig);
        RETURN_IF_STATUS_ERROR(status, "add kv config fail, indexName[%s]", pkConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

Status KVSchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                    config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(FillIndexConfigs(legacySchema, schema),
                           "fill index configs from legacy schema to tablet schema failed");
    FillTTLToSettings(legacySchema, schema);
    return Status::OK();
}
Status KVSchemaResolver::ResolveLegacySchema(const std::string& indexPath,
                                             indexlib::config::IndexPartitionSchema* schema)
{
    return Status::OK();
}
Status KVSchemaResolver::ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    auto configs = schema->GetIndexConfigs(index::KV_INDEX_TYPE_STR);
    if (configs.size() == 1) {
        const auto& kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(configs[0]);
        schema->SetPrimaryKeyIndexConfig(kvConfig);
    }
    if (auto status = ResolveGeneralValue(schema); !status.IsOK()) {
        return status;
    }
    auto [status, needResolvePkStoreconfig] = NeedResolvePkStoreConfig(schema);
    RETURN_IF_STATUS_ERROR(status, "get pk store config fail, table name[%s]", schema->GetTableName().c_str());
    if (needResolvePkStoreconfig) {
        RETURN_IF_STATUS_ERROR(ResolvePkStoreConfig(index::KV_INDEX_TYPE_STR, schema),
                               "resolve pk store for table[%s] failed", schema->GetTableName().c_str());
    }

    auto settings = std::make_shared<config::TTLSettings>();
    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetRuntimeSetting("enable_ttl", settings->enabled, settings->enabled));
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        schema->GetRuntimeSetting("ttl_from_doc", settings->ttlFromDoc, settings->ttlFromDoc));

    if (settings->enabled && settings->ttlFromDoc) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            schema->GetRuntimeSetting("ttl_field_name", settings->ttlField, settings->ttlField));
    }
    bool denyEmptyPrimaryKey = false;
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        schema->GetRuntimeSetting("deny_empty_pkey", denyEmptyPrimaryKey, denyEmptyPrimaryKey));
    configs = schema->GetIndexConfigs(index::KV_INDEX_TYPE_STR);
    bool ignoreEmptyPrimaryKey = configs.size() > 1;
    for (const auto& indexConfig : configs) {
        auto kvIndexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
        kvIndexConfig->SetTTLSettings(settings);
        kvIndexConfig->SetDenyEmptyPrimaryKey(denyEmptyPrimaryKey);
        kvIndexConfig->SetIgnoreEmptyPrimaryKey(ignoreEmptyPrimaryKey);
        if (schema->GetSchemaId() != DEFAULT_SCHEMAID) {
            kvIndexConfig->GetValueConfig()->DisableSimpleValue();
        }
    }
    return Status::OK();
}

Status KVSchemaResolver::ResolveGeneralValue(config::UnresolvedSchema* schema)
{
    const auto& indexConfigs = schema->GetIndexConfigs(index::KV_INDEX_TYPE_STR);
    if (indexConfigs.size() == 1) {
        const auto& kvConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfigs[0]);
        assert(kvConfig);
        const auto& valueConfig = kvConfig->GetValueConfig();
        std::vector<std::shared_ptr<config::FieldConfig>> fields;
        for (size_t i = 0; i < valueConfig->GetAttributeCount(); ++i) {
            const auto attrConfig = valueConfig->GetAttributeConfig(i);
            if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
                RETURN_IF_STATUS_ERROR(
                    schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                    "add [%s] into general_value failed", attrConfig->GetIndexName().c_str());
            }
        }
    }
    return Status::OK();
}

std::pair<Status, bool> KVSchemaResolver::NeedResolvePkStoreConfig(config::UnresolvedSchema* schema)
{
    auto [status, needStorePKValue] = schema->GetRuntimeSettings().GetValue<bool>(index::NEED_STORE_PK_VALUE);
    if (status.IsNotFound()) {
        return std::make_pair(Status::OK(), false);
    }
    RETURN2_IF_STATUS_ERROR(status, false, "read user defined param [need_store_pk_value] failed, table name [%s]",
                            schema->GetTableName().c_str());
    return std::make_pair(Status::OK(), needStorePKValue);
}

Status KVSchemaResolver::ResolvePkStoreConfig(const std::string& indexTypeStr, config::UnresolvedSchema* schema)
{
    if (schema->GetIndexConfig(index::KV_INDEX_TYPE_STR, index::KV_RAW_KEY_INDEX_NAME) != nullptr) {
        AUTIL_LOG(INFO, "index %s already exist, no need to resolve again", index::KV_RAW_KEY_INDEX_NAME.c_str());
        return Status::OK();
    }
    const auto& indexConfigs = schema->GetIndexConfigs(indexTypeStr);
    if (indexConfigs.size() != 1) {
        auto status = Status::Corruption("cannot support pk value store because of index config size[%ld] != 1,"
                                         "table name[%s]",
                                         indexConfigs.size(), schema->GetTableName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto kvIndexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfigs[0]);
    assert(kvIndexConfig);
    auto pkFieldConfig = schema->GetFieldConfig(kvIndexConfig->GetKeyFieldName());
    if (!pkFieldConfig) {
        auto status = Status::Corruption("get pk field config[%s] failed, table name[%s]",
                                         kvIndexConfig->GetKeyFieldName().c_str(), schema->GetTableName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    auto attributeConfig = std::make_shared<index::AttributeConfig>();
    auto status = attributeConfig->Init(pkFieldConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init attribute config failed, field [%s] table name [%s]",
                  pkFieldConfig->GetFieldName().c_str(), schema->GetTableName().c_str());
        return status;
    }
    std::vector<std::shared_ptr<index::AttributeConfig>> attributeConfigs;
    attributeConfigs.emplace_back(attributeConfig);
    auto valueConfig = std::make_shared<config::ValueConfig>();
    status = valueConfig->Init(attributeConfigs);
    RETURN_IF_STATUS_ERROR(status, "init kv value index fail, table name[%s]", schema->GetTableName().c_str());

    auto pkValueIndexConfig = std::make_shared<config::KVIndexConfig>(*kvIndexConfig);
    pkValueIndexConfig->SetIndexName(index::KV_RAW_KEY_INDEX_NAME);
    pkValueIndexConfig->SetFieldConfig(pkFieldConfig);
    pkValueIndexConfig->SetValueConfig(valueConfig);

    return schema->AddIndexConfig(pkValueIndexConfig);
}

} // namespace indexlibv2::table
