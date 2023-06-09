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
#include "indexlib/table/aggregation/AggregationSchemaResolver.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/aggregation/AggregationSchemaUtil.h"

namespace indexlibv2::table {
const std::string AggregationSchemaResolver::PRIMARY_KEY_HASH_NAME_SUFFIX = "__hash__";

Status AggregationSchemaResolver::Check(const config::TabletSchema& schema) { return Status::OK(); }

Status AggregationSchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                             config::UnresolvedSchema* schema)
{
    return Status::OK();
}

Status AggregationSchemaResolver::ResolveLegacySchema(const std::string& indexPath,
                                                      indexlib::config::IndexPartitionSchema* schema)
{
    return Status::OK();
}

Status AggregationSchemaResolver::ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    auto r = AggregationSchemaUtil::GetPrimaryKeyIndexName(schema);
    if (!r.IsOK() && !r.IsNotFound()) {
        return {r.steal_error()};
    } else if (r.IsNotFound()) {
        return Status::OK();
    }
    auto pkIndexName = std::move(r.steal_value());
    auto indexConfig = schema->GetIndexConfig(index::KV_INDEX_TYPE_STR, pkIndexName);
    auto pkIndexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
    if (!pkIndexConfig) {
        return Status::ConfigError("no primary index config, but the settings specified one");
    }
    schema->SetPrimaryKeyIndexConfig(pkIndexConfig);

    return MaybeSetupUnique(schema);
}

Status AggregationSchemaResolver::MaybeSetupUnique(config::UnresolvedSchema* schema)
{
    auto r1 = AggregationSchemaUtil::IsUniqueEnabled(schema);
    if (!r1.IsOK()) {
        return {r1.steal_error()};
    }
    bool enableUnique = r1.get();
    if (!enableUnique) {
        return Status::OK();
    }

    std::string uniqueFieldName;
    auto r2 = AggregationSchemaUtil::GetUniqueField(schema);
    if (!r2.IsOK()) {
        return {r2.steal_error()};
    } else {
        uniqueFieldName = std::move(r2.steal_value());
    }

    auto r3 = AggregationSchemaUtil::GetVersionField(schema);
    if (!r3.IsOK()) {
        return {r3.steal_error()};
    }
    auto versionFieldName = std::move(r3.steal_value());

    auto uniqueField = schema->GetFieldConfig(uniqueFieldName);
    auto versionField = schema->GetFieldConfig(versionFieldName);

    auto checkField = [](const std::shared_ptr<config::FieldConfig>& field) {
        return field && !field->IsMultiValue() && indexlib::IsIntegerField(field->GetFieldType());
    };
    if (!checkField(uniqueField) || !checkField(versionField)) {
        return Status::InternalError("unique/version field type invalid");
    }

    bool sortDeleteData = true;
    auto s = schema->GetSetting("sort_delete_data", sortDeleteData, true);
    if (!s.IsOK()) {
        return s;
    }

    auto aggIndexConfigs = schema->GetIndexConfigs(index::AGGREGATION_INDEX_TYPE_STR);
    for (const auto& indexConfig : aggIndexConfigs) {
        auto aggIndexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
        assert(aggIndexConfig);
        s = aggIndexConfig->SetUniqueField(uniqueField, versionField, sortDeleteData);
        if (!s.IsOK()) {
            return s;
        }
    }

    return Status::OK();
}

} // namespace indexlibv2::table
