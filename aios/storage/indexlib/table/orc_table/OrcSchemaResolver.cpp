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
#include "indexlib/table/orc_table/OrcSchemaResolver.h"

#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/orc/Common.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/primary_key/Common.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, OrcSchemaResolver);

Status OrcSchemaResolver::Check(const config::TabletSchema& schema)
{
    // TODO(makuo.mnb) implement
    return Status::OK();
}

Status OrcSchemaResolver::LegacySchemaToTabletSchema(const indexlib::config::IndexPartitionSchema& legacySchema,
                                                     config::UnresolvedSchema* schema)
{
    RETURN_IF_STATUS_ERROR(FillPkConfig(legacySchema, schema), "resolve pk config failed");
    RETURN_IF_STATUS_ERROR(FillOrcConfig(legacySchema, schema), "resolve orc config failed");
    return Status::OK();
}
Status OrcSchemaResolver::ResolveLegacySchema(const std::string& indexPath,
                                              indexlib::config::IndexPartitionSchema* schema)
{
    return Status::OK();
}
Status OrcSchemaResolver::ResolveTabletSchema(const std::string& indexPath, config::UnresolvedSchema* schema)
{
    ResolveGeneralizedValue(schema);
    const auto& configs = schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (!configs.empty()) {
        assert(configs.size() == 1);
        schema->SetPrimaryKeyIndexConfig(configs[0]);
    }
    return Status::OK();
}

void OrcSchemaResolver::ResolveGeneralizedValue(config::UnresolvedSchema* schema)
{
    std::vector<std::shared_ptr<config::FieldConfig>> fields;
    const auto& indexConfigs = schema->GetIndexConfigs(index::ORC_INDEX_TYPE_STR);
    assert(indexConfigs.size() == 1);
    const auto& orcConfig = std::dynamic_pointer_cast<config::OrcConfig>(indexConfigs[0]);
    assert(orcConfig);
    const auto& orcFields = orcConfig->GetFieldConfigs();
    fields = orcFields;
    schema->SetGeneralizedValueIndexFieldConfigs(fields);
}

Status OrcSchemaResolver::FillPkConfig(const indexlib::config::IndexPartitionSchema& legacySchema,
                                       config::UnresolvedSchema* schema)
{
    const auto& indexSchema = legacySchema.GetIndexSchema();
    if (indexSchema && indexSchema->GetPrimaryKeyIndexConfig()) {
        auto pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        if (auto config = schema->GetIndexConfig(index::PRIMARY_KEY_INDEX_TYPE_STR, pkConfig->GetIndexName())) {
            return Status::OK();
        }
        auto legacyConfig = std::dynamic_pointer_cast<indexlib::config::PrimaryKeyIndexConfig>(pkConfig);
        assert(legacyConfig);
        auto pkConfigV2 = legacyConfig->MakePrimaryIndexConfigV2();
        if (!pkConfigV2) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "make primary index config v2 failed");
        }
        std::shared_ptr<indexlibv2::config::IIndexConfig> pkConfigShared(std::move(pkConfigV2));
        auto status = schema->AddIndexConfig(std::shared_ptr<indexlibv2::config::IIndexConfig>(pkConfigShared));
        RETURN_IF_STATUS_ERROR(status, "add primary_key config fail, indexName[%s]", pkConfig->GetIndexName().c_str());
    }
    return Status::OK();
}
Status OrcSchemaResolver::FillOrcConfig(const indexlib::config::IndexPartitionSchema& legacySchema,
                                        config::UnresolvedSchema* schema)
{
    auto indexConfigs = schema->GetIndexConfigs(index::ORC_INDEX_TYPE_STR);
    if (!indexConfigs.empty()) {
        return Status::OK();
    }
    const auto& attrSchema = legacySchema.GetAttributeSchema();
    std::vector<std::shared_ptr<config::FieldConfig>> fields;
    if (attrSchema.get() != nullptr) {
        for (auto it = attrSchema->Begin(); it != attrSchema->End(); it++) {
            fields.push_back((*it)->GetFieldConfig());
        }
    }
    if (fields.empty()) {
        AUTIL_LOG(ERROR, "fields is empty, can not build orc index");
        return Status::ConfigError("fields is empty, can not build orc index");
    }
    auto orcConfig = std::make_shared<config::OrcConfig>(fields);
    orcConfig->SetLegacyWriterOptions();
    try {
        orcConfig->Check();
    } catch (...) {
        AUTIL_LOG(ERROR, "some field type is not supported");
        return Status::ConfigError("some field type is not supported");
    }
    RETURN_IF_STATUS_ERROR(schema->AddIndexConfig(orcConfig), "add orc config failed");
    return Status::OK();
}

} // namespace indexlibv2::table
