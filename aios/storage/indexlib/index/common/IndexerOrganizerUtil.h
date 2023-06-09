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
#pragma once

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/table/normal_table/virtual_attribute/Common.h"

namespace indexlibv2::framework {
class Segment;
} // namespace indexlibv2::framework

namespace indexlib::index {

class IndexerOrganizerUtil
{
public:
    // Given a valid segment, return one of disk indexer or dumping/building mem indexer.
    // Even if returned status is OK, all the output indexer pointers may be nullptr.
    // The only case as of 04/2023 that all the output indexer pointers are nullptr happens in alter table scenario.
    template <typename DiskIndexer, typename MemIndexer>
    static Status GetIndexer(indexlibv2::framework::Segment* segment,
                             std::shared_ptr<indexlibv2::config::IIndexConfig> indexConfig,
                             std::shared_ptr<DiskIndexer>* diskIndexer, std::shared_ptr<MemIndexer>* dumpingMemIndexer,
                             std::shared_ptr<MemIndexer>* buildingMemIndexer);

private:
    AUTIL_LOG_DECLARE();
};

template <typename DiskIndexer, typename MemIndexer>
Status IndexerOrganizerUtil::GetIndexer(indexlibv2::framework::Segment* segment,
                                        std::shared_ptr<indexlibv2::config::IIndexConfig> indexConfig,
                                        std::shared_ptr<DiskIndexer>* diskIndexer,
                                        std::shared_ptr<MemIndexer>* dumpingMemIndexer,
                                        std::shared_ptr<MemIndexer>* buildingMemIndexer)
{
    const std::string& indexName = indexConfig->GetIndexName();
    const std::string& indexType = indexConfig->GetIndexType();
    auto [status, indexer] = segment->GetIndexer(indexType, indexName);
    if (!status.IsOK()) {
        assert(indexer == nullptr);
        std::shared_ptr<indexlibv2::config::TabletSchema> segmentSchema = segment->GetSegmentSchema();
        if (status.IsNotFound()) {
            if (segmentSchema != nullptr && segmentSchema->GetIndexConfig(indexType, indexName) == nullptr) {
                // This case happens in alter table scenario.
                AUTIL_LOG(INFO, "Indexer for [%s] in segment [%d] does not support update.", indexName.c_str(),
                          segment->GetSegmentId());
                return Status::OK();
            }
            if (indexType == indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR ||
                indexType == OPERATION_LOG_INDEX_TYPE_STR) {
                AUTIL_LOG(INFO, "Indexer for [%s] in segment [%d] does not exist.", indexName.c_str(),
                          segment->GetSegmentId());
                return Status::OK();
            }
        }
        return Status::InternalError("No indexer for [%s] in segment [%d] ", indexName.c_str(),
                                     segment->GetSegmentId());
    }
    assert(indexer != nullptr);
    auto segStatus = segment->GetSegmentStatus();
    if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
        std::shared_ptr<DiskIndexer> castedIndexer = nullptr;
        castedIndexer = std::dynamic_pointer_cast<DiskIndexer>(indexer);
        if (castedIndexer == nullptr) {
            return Status::InternalError("Indexer is not diskIndexer for [%s] in segment [%d] ", indexName.c_str(),
                                         segment->GetSegmentId());
        }
        *diskIndexer = castedIndexer;
        return Status::OK();
    }
    std::shared_ptr<MemIndexer> castedIndexer = nullptr;
    castedIndexer = std::dynamic_pointer_cast<MemIndexer>(indexer);
    if (castedIndexer == nullptr) {
        return Status::InternalError("Indexer is not memIndexer for [%s] in segment [%d] ", indexName.c_str(),
                                     segment->GetSegmentId());
    }
    if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_DUMPING) {
        *dumpingMemIndexer = castedIndexer;
        return Status::OK();
    }
    if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
        *buildingMemIndexer = castedIndexer;
        return Status::OK();
    }
    return Status::InternalError("Invalid segment status for [%s] in segment [%d] ", indexName.c_str(),
                                 segment->GetSegmentId());
}

} // namespace indexlib::index
