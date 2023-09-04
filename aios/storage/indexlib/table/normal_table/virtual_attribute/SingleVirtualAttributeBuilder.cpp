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
#include "indexlib/table/normal_table/virtual_attribute/SingleVirtualAttributeBuilder.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, SingleVirtualAttributeBuilder);

SingleVirtualAttributeBuilder::SingleVirtualAttributeBuilder(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
    : SingleAttributeBuilder(schema)
{
}

SingleVirtualAttributeBuilder::~SingleVirtualAttributeBuilder() {}

Status
SingleVirtualAttributeBuilder::InitConfigRelated(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    const auto& pkConfigs = _schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR);
    std::shared_ptr<indexlibv2::config::IIndexConfig> pkConfig;
    if (pkConfigs.size() > 0) {
        pkConfig = pkConfigs[0];
    }
    std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>> attrConfigs;
    attrConfigs.push_back(indexConfig);

    auto virtualAttributeConfig = std::dynamic_pointer_cast<indexlibv2::table::VirtualAttributeConfig>(indexConfig);
    if (virtualAttributeConfig == nullptr) {
        return Status::InvalidArgs("Invalid indexConfig, name: %s", indexConfig->GetIndexName().c_str());
    }
    auto attributeConfig =
        std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(virtualAttributeConfig->GetAttributeConfig());
    if (attributeConfig == nullptr) {
        return Status::InvalidArgs("Invalid indexConfig, name: %s", indexConfig->GetIndexName().c_str());
    }
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> fieldConfigs = {attributeConfig->GetFieldConfig()};
    _extractor.Init(pkConfig, attrConfigs, fieldConfigs);

    return index::AttributeIndexerOrganizerUtil::CreateSingleAttributeBuildInfoHolder<
        indexlibv2::table::VirtualAttributeDiskIndexer, indexlibv2::table::VirtualAttributeMemIndexer>(
        indexConfig, attributeConfig, &_buildInfoHolder);
}

} // namespace indexlib::table
