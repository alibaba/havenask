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
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AdapterIgnoreFieldCalculator);

AdapterIgnoreFieldCalculator::AdapterIgnoreFieldCalculator() {}

AdapterIgnoreFieldCalculator::~AdapterIgnoreFieldCalculator() {}

bool AdapterIgnoreFieldCalculator::Init(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                        const framework::TabletData* tabletData)
{
    assert(kvIndexConfig && tabletData);
    schemaid_t beginSchemaId = tabletData->GetOnDiskVersionReadSchema() != nullptr
                                   ? tabletData->GetOnDiskVersionReadSchema()->GetSchemaId()
                                   : tabletData->GetOnDiskVersion().GetReadSchemaId();
    schemaid_t endSchemaId = tabletData->GetWriteSchema() != nullptr ? tabletData->GetWriteSchema()->GetSchemaId()
                                                                     : tabletData->GetOnDiskVersion().GetSchemaId();
    auto slice = tabletData->CreateSlice();
    for (auto it = slice.rbegin(); it != slice.rend(); it++) {
        auto segment = it->get();
        schemaid_t schemaId = segment->GetSegmentSchema()->GetSchemaId();
        if (tabletData->GetTabletSchema(schemaId) == nullptr) {
            AUTIL_LOG(ERROR, "get tablet schema [%d] fail from tabletData", schemaId);
            return false;
        }
        beginSchemaId = std::min(beginSchemaId, schemaId);
        endSchemaId = std::max(endSchemaId, schemaId);
    }
    auto schemas = tabletData->GetAllTabletSchema(beginSchemaId, endSchemaId);
    if (schemas.size() < 2) { // no mid schema exist, no need calculate drop fields
        return true;
    }
    auto indexType = kvIndexConfig->GetIndexType();
    auto indexName = kvIndexConfig->GetIndexName();
    for (size_t i = 1; i < schemas.size(); i++) {
        auto current = schemas[i - 1];
        auto target = schemas[i];
        std::vector<std::string> removeFields;
        auto currentIndexConfig =
            std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(current->GetIndexConfig(indexType, indexName));
        auto targetIndexConfig =
            std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(target->GetIndexConfig(indexType, indexName));
        assert(currentIndexConfig && targetIndexConfig);
        auto targetValueConfig = targetIndexConfig->GetValueConfig();
        std::set<std::string> fieldSet;
        for (size_t j = 0; j < targetValueConfig->GetAttributeCount(); j++) {
            fieldSet.insert(targetValueConfig->GetAttributeConfig(j)->GetAttrName());
        }
        auto currentValueConfig = currentIndexConfig->GetValueConfig();
        for (size_t j = 0; j < currentValueConfig->GetAttributeCount(); j++) {
            auto& attrName = currentValueConfig->GetAttributeConfig(j)->GetAttrName();
            if (fieldSet.find(attrName) == fieldSet.end()) { // remove in target
                removeFields.push_back(attrName);
            }
        }
        _removeFieldRoadMap.insert(std::make_pair(target->GetSchemaId(), std::move(removeFields)));
    }
    return true;
}

std::vector<std::string> AdapterIgnoreFieldCalculator::GetIgnoreFields(schemaid_t beginSchemaId, schemaid_t endSchemaId)
{
    assert(beginSchemaId <= endSchemaId);
    std::set<std::string> ignoreFields;
    auto iter = _removeFieldRoadMap.upper_bound(beginSchemaId);
    for (; iter != _removeFieldRoadMap.end(); iter++) {
        if (iter->first >= endSchemaId) {
            break;
        }
        auto& removeFields = iter->second;
        for (auto& field : removeFields) {
            ignoreFields.insert(field);
        }
    }
    return std::vector<std::string>(ignoreFields.begin(), ignoreFields.end());
}

} // namespace indexlibv2::index
