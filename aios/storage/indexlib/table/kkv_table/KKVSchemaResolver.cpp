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
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"

#include <cassert>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/config/TTLSettings.h"
#include "indexlib/table/kv_table/KVSchemaResolver.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVSchemaResolver);

KKVSchemaResolver::KKVSchemaResolver() {}

KKVSchemaResolver::~KKVSchemaResolver() {}

Status KKVSchemaResolver::Check(const config::TabletSchema& schema) const
{
    auto indexConfigs = schema.GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    if (indexConfigs.empty()) {
        return Status::ConfigError("no valid kkv index config");
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

void KKVSchemaResolver::FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
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

void KKVSchemaResolver::FillNeedStorePKValueToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                       config::UnresolvedSchema* schema)
{
    auto [statusLegacy, needStorePKValue] = legacySchema.GetUserDefinedParam().Get<bool>("need_store_pk_value");
    if (!statusLegacy.IsOK()) {
        return;
    }
    auto [status, _] = schema->GetRuntimeSettings().GetValue<bool>("need_store_pk_value");
    if (status.IsOK()) {
        // tablet schema setting has need_store_pk_value config, not use legacy schema
        return;
    }
    schema->SetRuntimeSetting("need_store_pk_value", needStorePKValue, false);
}

Status KKVSchemaResolver::FillIndexConfigs(const indexlib::config::IndexPartitionSchema& legacySchema,
                                           config::UnresolvedSchema* schema)
{
    assert(schema);
    auto indexConfigs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    if (!indexConfigs.empty()) {
        return Status::OK();
    }
    const auto& indexSchema = legacySchema.GetIndexSchema();
    if (indexSchema != nullptr && indexSchema->HasPrimaryKeyIndex()) {
        auto pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        auto legacyConfig = std::dynamic_pointer_cast<indexlib::config::KKVIndexConfig>(pkConfig);
        assert(legacyConfig);
        auto kkvIndexConfig = std::shared_ptr<indexlibv2::config::KKVIndexConfig>(legacyConfig->MakeKKVIndexConfigV2());
        assert(kkvIndexConfig);
        auto valueConfig = kkvIndexConfig->GetValueConfig();
        assert(valueConfig);
        valueConfig->EnableCompactFormat(true);
        auto status = schema->AddIndexConfig(kkvIndexConfig);
        RETURN_IF_STATUS_ERROR(status, "add kkv config fail, indexName[%s]", pkConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

// TODO(qisa.cb) all copy from KVSchemaResolver
Status KKVSchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                     config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(FillIndexConfigs(legacySchema, schema),
                           "fill index configs from legacy schema to tablet schema failed");
    FillTTLToSettings(legacySchema, schema);
    FillNeedStorePKValueToSettings(legacySchema, schema);
    return Status::OK();
}
Status KKVSchemaResolver::ResolveLegacySchema(const std::string& indexPath,
                                              indexlib::config::IndexPartitionSchema* schema)
{
    return Status::OK();
}
Status KKVSchemaResolver::ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    auto configs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    if (configs.size() == 1) {
        const auto& kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(configs[0]);
        schema->SetPrimaryKeyIndexConfig(kkvConfig);
    }
    RETURN_IF_STATUS_ERROR(ResolveGeneralValue(schema), "ResolveGeneralValue failed for %s",
                           schema->GetTableName().c_str());
    RETURN_IF_STATUS_ERROR(ResolvePkStoreConfig(schema), "ResolvePkStoreConfig failed for %s",
                           schema->GetTableName().c_str());

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
    bool denyEmptySKey = false;
    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetRuntimeSetting("deny_empty_skey", denyEmptySKey, denyEmptySKey));
    configs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    bool ignoreEmptyPrimaryKey = configs.size() > 1;
    for (const auto& indexConfig : configs) {
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        kkvIndexConfig->SetTTLSettings(settings);
        kkvIndexConfig->SetDenyEmptyPrimaryKey(denyEmptyPrimaryKey);
        kkvIndexConfig->SetDenyEmptySuffixKey(denyEmptySKey);
        kkvIndexConfig->SetIgnoreEmptyPrimaryKey(ignoreEmptyPrimaryKey);
        if (schema->GetSchemaId() != DEFAULT_SCHEMAID) {
            kkvIndexConfig->GetValueConfig()->DisableSimpleValue();
        }
    }
    return Status::OK();
}

Status KKVSchemaResolver::ResolvePkStoreConfig(config::UnresolvedSchema* schema)
{
    auto [status, needResolvePkStoreconfig] = KVSchemaResolver::NeedResolvePkStoreConfig(schema);
    RETURN_IF_STATUS_ERROR(status, "get pk store config fail, table name[%s]", schema->GetTableName().c_str());
    if (needResolvePkStoreconfig) {
        return KVSchemaResolver::ResolvePkStoreConfig(index::KKV_INDEX_TYPE_STR, schema);
    }
    return Status::OK();
}

Status KKVSchemaResolver::ResolveGeneralValue(config::UnresolvedSchema* schema)
{
    const auto& indexConfigs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    if (indexConfigs.size() == 1) {
        const auto& kkvConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfigs[0]);
        assert(kkvConfig);

        const auto& valueConfig = kkvConfig->GetValueConfig();
        size_t attributeCount = valueConfig->GetAttributeCount();
        for (size_t i = 0; i < attributeCount; ++i) {
            const auto& attrConfig = valueConfig->GetAttributeConfig(i);
            if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig->GetIndexName())) {
                RETURN_IF_STATUS_ERROR(
                    schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                    "add [%s] into general_value failed", attrConfig->GetIndexName().c_str());
            }
        }
        const auto& skey = kkvConfig->GetSuffixFieldConfig();
        if (!schema->GetIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, skey->GetFieldName())) {
            auto attrConfig = std::make_shared<index::AttributeConfig>();
            RETURN_IF_STATUS_ERROR(attrConfig->Init(skey), "init skey [%s] attribute config failed",
                                   skey->GetFieldName().c_str());
            RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR, attrConfig),
                                   "add [%s] into general_value failed", attrConfig->GetIndexName().c_str());
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::table
