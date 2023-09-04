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
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMerger.h"

#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SectionAttributeMerger);

SectionAttributeMerger::SectionAttributeMerger() {}

SectionAttributeMerger::~SectionAttributeMerger() {}

std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
SectionAttributeMerger::GetIndexerFromSegment(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
                                              const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig)
{
    assert(attrConfig->GetIndexCommonPath() == indexlib::index::INVERTED_INDEX_PATH);
    auto invertedIndexName =
        indexlibv2::config::SectionAttributeConfig::SectionAttributeNameToIndexName(attrConfig->GetAttrName());
    auto [status, diskIndexer] = segment->GetIndexer(indexlib::index::INVERTED_INDEX_TYPE_STR, invertedIndexName);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get inverted indexer faile for section attribute [%s]",
                            attrConfig->GetIndexName().c_str());
    const auto& invertedDiskIndexer = std::dynamic_pointer_cast<indexlib::index::IInvertedDiskIndexer>(diskIndexer);
    if (invertedDiskIndexer && invertedDiskIndexer->GetSectionAttributeDiskIndexer()) {
        return {Status::OK(), invertedDiskIndexer->GetSectionAttributeDiskIndexer()};
    }
    AUTIL_LOG(ERROR, "unknown inverted disk indexer");
    return {Status::Corruption("unknown inverted disk indexer"), nullptr};
}

} // namespace indexlib::index
