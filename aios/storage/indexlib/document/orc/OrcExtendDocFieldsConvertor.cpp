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
#include "indexlib/document/orc/OrcExtendDocFieldsConvertor.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/orc/OrcConfig.h"

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, OrcExtendDocFieldsConvertor);

OrcExtendDocFieldsConvertor::OrcExtendDocFieldsConvertor(
    const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
    : ExtendDocFieldsConvertor(schema)
{
}

OrcExtendDocFieldsConvertor::~OrcExtendDocFieldsConvertor() {}

std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>
OrcExtendDocFieldsConvertor::GetAttributeConfigs() const
{
    std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>> result;
    const auto& configs = _schema->GetIndexConfigs(indexlibv2::index::ORC_INDEX_TYPE_STR);
    if (configs.empty()) {
        return {};
    }
    assert(configs.size() == 1);
    const auto& orcConfig = std::dynamic_pointer_cast<indexlibv2::config::OrcConfig>(configs[0]);
    assert(orcConfig);
    const auto& fieldConfigs = orcConfig->GetFieldConfigs();
    result.reserve(fieldConfigs.size());
    for (const auto& fieldConfig : fieldConfigs) {
        std::shared_ptr<indexlibv2::config::AttributeConfig> attrConfig =
            std::make_unique<indexlibv2::config::AttributeConfig>();
        attrConfig->Init(fieldConfig);
        result.push_back(attrConfig);
    }
    return result;
}

} // namespace indexlib::document
