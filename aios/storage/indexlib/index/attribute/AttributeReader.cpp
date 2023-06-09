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
#include "indexlib/index/attribute/AttributeReader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeReader);

namespace {
bool UseDefaultValue(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                     std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>> schemas)
{
    if (schemas.size() <= 1) {
        return false;
    }
    for (const auto& schema : schemas) {
        if (schema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName()) == nullptr) {
            return true;
        }
    }
    return false;
}
} // namespace

Status AttributeReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                             const framework::TabletData* tabletData)
{
    assert(indexConfig != nullptr);
    AUTIL_LOG(DEBUG, "Start opening attribute(%s).", indexConfig->GetIndexName().c_str());
    std::vector<IndexerInfo> indexers;
    auto segments = tabletData->CreateSlice();
    auto readSchemaId = tabletData->GetOnDiskVersionReadSchema()->GetSchemaId();
    docid_t baseDocId = 0;

    for (const auto& segment : segments) {
        auto segmentSchemaId = segment->GetSegmentSchema()->GetSchemaId();
        auto docCount = segment->GetSegmentInfo()->GetDocCount();
        if (segment->GetSegmentStatus() == framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            continue; // there is no document in current segment, so do nothing
        }

        auto schemas = tabletData->GetAllTabletSchema(std::min(readSchemaId, segmentSchemaId),
                                                      std::max(readSchemaId, segmentSchemaId));
        bool useDefaultValue = UseDefaultValue(indexConfig, schemas);
        if (useDefaultValue) {
            switch (segment->GetSegmentStatus()) {
            case framework::Segment::SegmentStatus::ST_BUILT: {
                indexers.emplace_back(nullptr, segment, baseDocId);
                break;
            }
            case framework::Segment::SegmentStatus::ST_DUMPING:
            case framework::Segment::SegmentStatus::ST_BUILDING: {
                _fillDefaultAttrReader = true;
                AUTIL_LOG(INFO, "attribute [%s] in segment [%d] use default attribute reader",
                          indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                break;
            }
            default:
                assert(false);
            }
        } else {
            auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
            if (!status.IsOK()) {
                RETURN_STATUS_ERROR(InternalError, "no indexer for [%s] in segment [%d]",
                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
            }
            indexers.emplace_back(indexer, segment, baseDocId);
        }
        baseDocId += docCount;
    }
    auto status = DoOpen(indexConfig, indexers);
    AUTIL_LOG(DEBUG, "Finish opening attribute(%s).", indexConfig->GetIndexName().c_str());
    return status;
}

Status AttributeReader::OpenWithSegments(const std::shared_ptr<config::AttributeConfig>& indexConfig,
                                         std::vector<std::shared_ptr<framework::Segment>> segments)
{
    assert(indexConfig != nullptr);
    AUTIL_LOG(DEBUG, "Start opening attribute(%s).", indexConfig->GetIndexName().c_str());
    std::vector<IndexerInfo> indexers;
    docid_t baseDocId = 0;

    for (const auto& segment : segments) {
        auto docCount = segment->GetSegmentInfo()->GetDocCount();
        if (segment->GetSegmentStatus() == framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            continue; // there is no document in current segment, so do nothing
        }

        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!status.IsOK()) {
            RETURN_STATUS_ERROR(InternalError, "no indexer for [%s] in segment [%d]",
                                indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        indexers.emplace_back(indexer, segment, baseDocId);
        baseDocId += docCount;
    }
    auto status = DoOpen(indexConfig, indexers);
    AUTIL_LOG(DEBUG, "Finish opening attribute(%s).", indexConfig->GetIndexName().c_str());
    return status;
}

} // namespace indexlibv2::index
