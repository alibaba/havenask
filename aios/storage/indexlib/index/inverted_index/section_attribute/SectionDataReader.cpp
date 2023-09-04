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
#include "indexlib/index/inverted_index/section_attribute/SectionDataReader.h"

#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/IInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMemIndexer.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::Segment;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, SectionDataReader);

Status SectionDataReader::DoOpen(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                 const std::vector<IndexerInfo>& indexers)
{
    auto packageIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(indexConfig);
    assert(packageIndexConfig != nullptr);
    AUTIL_LOG(INFO, "Start opening section attribute(%s).", packageIndexConfig->GetIndexName().c_str());

    auto sectionAttrConf = packageIndexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    _attrConfig = sectionAttrConf->CreateAttributeConfig(packageIndexConfig->GetIndexName());
    if (_attrConfig == nullptr) {
        auto status =
            Status::InternalError("create attr config failed, index name [%s]", indexConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        assert(false);
        return status;
    }
    _fieldPrinter.Init(_attrConfig->GetFieldConfig());
    assert(packageIndexConfig->GetShardingType() != indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING);

    std::vector<IndexerInfo> memIndexers;
    // built segment
    for (auto& [indexer, segment, baseDocId] : indexers) {
        auto segId = segment->GetSegmentId();
        auto segStatus = segment->GetSegmentStatus();
        auto docCount = segment->GetSegmentInfo()->docCount;

        if (Segment::SegmentStatus::ST_BUILT == segStatus) {
            auto invertedDiskIndexer = std::dynamic_pointer_cast<IInvertedDiskIndexer>(indexer);
            if (!invertedDiskIndexer) {
                RETURN_IF_STATUS_ERROR(Status::InternalError(),
                                       "cast to IInvertedDiskIndexer failed, index name [%s] segment [%d]",
                                       indexConfig->GetIndexName().c_str(), segId);
            }
            auto diskIndexer = invertedDiskIndexer->GetSectionAttributeDiskIndexer();
            const auto& sectionAttrDiskIndexer =
                std::dynamic_pointer_cast<indexlibv2::index::MultiValueAttributeDiskIndexer<char>>(diskIndexer);
            if (!sectionAttrDiskIndexer) {
                RETURN_IF_STATUS_ERROR(Status::InternalError(),
                                       "index for [%s] in segment [%d] has no section attribute disk indexer",
                                       indexConfig->GetIndexName().c_str(), segId);
            }
            _isIntegrated = _isIntegrated && (sectionAttrDiskIndexer->GetBaseAddress() != nullptr);
            _onDiskIndexers.emplace_back(sectionAttrDiskIndexer);
        } else if (Segment::SegmentStatus::ST_DUMPING == segStatus ||
                   Segment::SegmentStatus::ST_BUILDING == segStatus) {
            memIndexers.emplace_back(indexer, segment, baseDocId);
            continue;
        }
        _segmentDocCount.emplace_back(docCount);
    }

    // building segment
    for (auto& [indexer, segment, baseDocId] : memIndexers) {
        auto segId = segment->GetSegmentId();
        auto invertedMemIndexer = std::dynamic_pointer_cast<IInvertedMemIndexer>(indexer);
        if (!invertedMemIndexer) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(),
                                   "cast to IInvertedMemIndexer failed, index name [%s] segment [%d]",
                                   indexConfig->GetIndexName().c_str(), segId);
        }
        auto sectionAttrMemReader = std::dynamic_pointer_cast<indexlibv2::index::MultiValueAttributeMemReader<char>>(
            invertedMemIndexer->CreateSectionAttributeMemReader());
        if (nullptr == sectionAttrMemReader) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "index for [%s] in segment [%d] has no memindexer",
                                   indexConfig->GetIndexName().c_str(), segId);
        }
        _memReaders.emplace_back(std::make_pair(baseDocId, sectionAttrMemReader));
    }
    return Status::OK();
}

} // namespace indexlib::index
