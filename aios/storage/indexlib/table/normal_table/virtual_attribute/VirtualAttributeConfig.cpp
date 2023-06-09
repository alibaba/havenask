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
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeConfig.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, VirtualAttributeConfig);

VirtualAttributeConfig::VirtualAttributeConfig(const std::shared_ptr<indexlibv2::config::AttributeConfig>& attrConfig)
    : _attrConfig(attrConfig)
{
}
VirtualAttributeConfig::~VirtualAttributeConfig() {}
const std::string& VirtualAttributeConfig::GetIndexType() const { return VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR; }
const std::string& VirtualAttributeConfig::GetIndexName() const { return _attrConfig->GetIndexName(); }
void VirtualAttributeConfig::Check() const { return _attrConfig->Check(); }
std::shared_ptr<config::IIndexConfig> VirtualAttributeConfig::GetAttributeConfig() const { return _attrConfig; }
std::vector<std::shared_ptr<config::FieldConfig>> VirtualAttributeConfig::GetFieldConfigs() const { return {}; }

std::vector<std::string> VirtualAttributeConfig::GetIndexPath() const { return _attrConfig->GetIndexPath(); }
Status VirtualAttributeConfig::CheckCompatible(const IIndexConfig* other) const { return Status::OK(); }

} // namespace indexlibv2::table
