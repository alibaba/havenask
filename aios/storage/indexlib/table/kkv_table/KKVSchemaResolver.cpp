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

Status KKVSchemaResolver::Check(const config::TabletSchema& schema)
{
    // TODO(makuo.mnb) implement
    return Status::OK();
}

void KKVSchemaResolver::FillTTLToSettings(const indexlib::config::IndexPartitionSchema& legacySchema,
                                          config::UnresolvedSchema* schema)
{
    if (!legacySchema.TTLEnabled()) {
        return;
    }
    auto [status, ttlEnable] = schema->GetSetting<bool>("enable_ttl");
    auto [status1, fieldName] = schema->GetSetting<std::string>("ttl_field_name");
    auto [status2, defaultTTL] = schema->GetSetting<int64_t>("default_ttl");
    auto [status3, ttlFromDoc] = schema->GetSetting<bool>("ttl_from_doc");
    if (status.IsOK() || status1.IsOK() || status2.IsOK() || status3.IsOK()) {
        // tablet schema setting has ttl config, not use legacy schema
        return;
    }

    bool ret = schema->SetSetting("enable_ttl", true, false);
    ret = schema->SetSetting("ttl_field_name", legacySchema.GetTTLFieldName(), false) && ret;
    ret = schema->SetSetting("default_ttl", legacySchema.GetDefaultTTL(), false) && ret;
    ret = schema->SetSetting("ttl_from_doc", legacySchema.TTLFromDoc(), false) && ret;
    assert(ret);
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
    ResolveGeneralizedValue(schema);
    RETURN_IF_STATUS_ERROR(ResolvePkStoreConfig(schema), "ResolvePkStoreConfig failed for %s",
                           schema->GetTableName().c_str());

    auto settings = std::make_shared<config::TTLSettings>();

    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetSetting("enable_ttl", settings->enabled, settings->enabled));
    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetSetting("ttl_from_doc", settings->ttlFromDoc, settings->ttlFromDoc));

    if (settings->enabled && settings->ttlFromDoc) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetSetting("ttl_field_name", settings->ttlField, settings->ttlField));
    }
    bool denyEmptyPrimaryKey = false;
    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetSetting("deny_empty_pkey", denyEmptyPrimaryKey, denyEmptyPrimaryKey));
    bool denyEmptySKey = false;
    RETURN_STATUS_DIRECTLY_IF_ERROR(schema->GetSetting("deny_empty_skey", denyEmptySKey, denyEmptySKey));
    configs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    bool ignoreEmptyPrimaryKey = configs.size() > 1;
    for (const auto& indexConfig : configs) {
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        kkvIndexConfig->SetTTLSettings(settings);
        kkvIndexConfig->SetDenyEmptyPrimaryKey(denyEmptyPrimaryKey);
        kkvIndexConfig->SetDenyEmptySuffixKey(denyEmptySKey);
        kkvIndexConfig->SetIgnoreEmptyPrimaryKey(ignoreEmptyPrimaryKey);
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

void KKVSchemaResolver::ResolveGeneralizedValue(config::UnresolvedSchema* schema)
{
    const auto& indexConfigs = schema->GetIndexConfigs(index::KKV_INDEX_TYPE_STR);
    if (indexConfigs.size() == 1) {
        const auto& kkvConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfigs[0]);
        assert(kkvConfig);

        const auto& valueConfig = kkvConfig->GetValueConfig();
        size_t attributeCount = valueConfig->GetAttributeCount();
        std::vector<std::shared_ptr<config::FieldConfig>> fields;
        fields.reserve(attributeCount);
        for (size_t i = 0; i < attributeCount; ++i) {
            fields.emplace_back(valueConfig->GetAttributeConfig(i)->GetFieldConfig());
        }
        const auto& skey = kkvConfig->GetSuffixFieldConfig();
        // TODO(xinfei.sxf) kkv index config support this branch judge to reduce this cost
        auto iter = find_if(fields.begin(), fields.end(),
                            [&skey](auto field) { return field->GetFieldName() == skey->GetFieldName(); });
        if (iter == fields.end()) {
            fields.push_back(skey);
        }
        schema->SetGeneralizedValueIndexFieldConfigs(fields);
    }
}

} // namespace indexlibv2::table
