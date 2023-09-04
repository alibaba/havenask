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
#include "indexlib/index/pack_attribute/PackAttributeDiskIndexer.h"

#include "indexlib/index/pack_attribute/PackAttributeConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeDiskIndexer);

PackAttributeDiskIndexer::PackAttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                                   const IndexerParameter& indexerParameter)
    : MultiValueAttributeDiskIndexer<char>(attributeMetrics, indexerParameter)
{
}

PackAttributeDiskIndexer::~PackAttributeDiskIndexer() {}

// Status PackAttributeDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
//                                       const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
// {
//     _packAttrConfig = std::dynamic_pointer_cast<PackAttributeConfig>(indexConfig);
//     _updatable = _packAttrConfig->IsPackAttributeUpdatable();
//     const auto& attrConfig = _packAttrConfig->CreateAttributeConfig();
//     if (auto status = MultiValueAttributeDiskIndexer<char>::Open(attrConfig, indexDirectory); !status.IsOK()) {
//         return status;
//     }
//     return Status::OK();
// }

} // namespace indexlibv2::index
