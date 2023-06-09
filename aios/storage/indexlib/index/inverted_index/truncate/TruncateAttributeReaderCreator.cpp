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
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, TruncateAttributeReaderCreator);

TruncateAttributeReaderCreator::TruncateAttributeReaderCreator(
    const std::shared_ptr<indexlibv2::config::TabletSchema>& tabletSchema,
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper)
    : _tabletSchema(tabletSchema)
    , _docMapper(docMapper)
    , _segmentMergeInfos(segmentMergeInfos)
{
}
std::shared_ptr<TruncateAttributeReader> TruncateAttributeReaderCreator::Create(const std::string& attrName)
{
    std::lock_guard<std::mutex> guard(_mutex);
    auto it = _truncateAttributeReaders.find(attrName);
    if (it != _truncateAttributeReaders.end()) {
        return it->second;
    }
    std::shared_ptr<TruncateAttributeReader> reader = CreateAttributeReader(attrName);
    if (reader != nullptr) {
        _truncateAttributeReaders.insert(it, std::make_pair(attrName, reader));
    }
    return reader;
}

std::shared_ptr<TruncateAttributeReader>
TruncateAttributeReaderCreator::CreateAttributeReader(const std::string& fieldName)
{
    auto attrConfig = GetAttributeConfig(fieldName);
    if (attrConfig == nullptr) {
        AUTIL_LOG(ERROR, "get attribute config failed, field[%s]", fieldName.c_str());
        return nullptr;
    }

    auto attrReader = std::make_shared<TruncateAttributeReader>(_docMapper, attrConfig);
    for (const auto& srcSegment : _segmentMergeInfos.srcSegments) {
        if (srcSegment.segment->GetSegmentInfo()->GetDocCount() == 0) {
            continue;
        }
        auto [st, diskIndexer] = srcSegment.segment->GetIndexer(attrConfig->GetIndexType(), attrConfig->GetIndexName());
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "get indexer failed, name[%s]", attrConfig->GetIndexName().c_str());
            return nullptr;
        }
        assert(diskIndexer != nullptr);
        auto attributeDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::AttributeDiskIndexer>(diskIndexer);
        assert(attributeDiskIndexer != nullptr);
        attrReader->AddAttributeDiskIndexer(srcSegment.segment->GetSegmentId(), attributeDiskIndexer);
    }
    return attrReader;
}

std::shared_ptr<indexlibv2::config::AttributeConfig>
TruncateAttributeReaderCreator::GetAttributeConfig(const std::string& fieldName) const
{
    auto indexConfig = _tabletSchema->GetIndexConfig(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
    if (indexConfig == nullptr) {
        AUTIL_LOG(ERROR, "get attribute config failed, fieldName[%s]", fieldName.c_str());
        return nullptr;
    }
    return std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
}
} // namespace indexlib::index
