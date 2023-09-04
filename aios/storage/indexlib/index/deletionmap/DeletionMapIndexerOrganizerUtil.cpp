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
#include "indexlib/index/deletionmap/DeletionMapIndexerOrganizerUtil.h"

#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::Segment;
using indexlibv2::index::DeletionMapDiskIndexer;
using indexlibv2::index::DeletionMapMemIndexer;
} // namespace
AUTIL_LOG_SETUP(indexlib.index, DeletionMapIndexerOrganizerUtil);

Status DeletionMapIndexerOrganizerUtil::Delete(docid_t docId, SingleDeletionMapBuildInfoHolder* buildInfoHolder)
{
    const std::vector<docid_t>& segmentBaseDocids = buildInfoHolder->segmentBaseDocids;
    if (docId < segmentBaseDocids.front()) {
        return Status::InvalidArgs("Invalid docid to delete: %d", docId);
    }
    int idx =
        std::upper_bound(segmentBaseDocids.begin(), segmentBaseDocids.end(), docId) - segmentBaseDocids.begin() - 1;
    docid_t inSegmentDocid = docId - segmentBaseDocids[idx];
    if (idx < buildInfoHolder->diskIndexers.size()) {
        std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer> diskIndexer = nullptr;
        RETURN_STATUS_DIRECTLY_IF_ERROR(
            SingleDeletionMapBuildInfoHolder::GetDiskIndexer(buildInfoHolder, idx, &diskIndexer));
        return diskIndexer->Delete(inSegmentDocid);
    }
    if (idx < buildInfoHolder->diskIndexers.size() + buildInfoHolder->dumpingIndexers.size()) {
        return buildInfoHolder->dumpingIndexers[idx - buildInfoHolder->diskIndexers.size()].second->Delete(
            inSegmentDocid);
    }
    return buildInfoHolder->buildingIndexer->Delete(inSegmentDocid);
}

Status DeletionMapIndexerOrganizerUtil::InitFromTabletData(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    const indexlibv2::framework::TabletData* tabletData, SingleDeletionMapBuildInfoHolder* buildInfoHolder)
{
    buildInfoHolder->deletionMapConfig = std::dynamic_pointer_cast<indexlibv2::index::DeletionMapConfig>(indexConfig);
    assert(buildInfoHolder->deletionMapConfig != nullptr);

    std::vector<std::tuple<segmentid_t, std::shared_ptr<indexlibv2::index::IIndexer>,
                           indexlibv2::framework::Segment::SegmentStatus, size_t>>
        indexers;
    auto slice = tabletData->CreateSlice();
    for (auto segment : slice) {
        Segment::SegmentStatus segmentStatus = segment->GetSegmentStatus();
        segmentid_t segId = segment->GetSegmentId();
        size_t docCount = segment->GetSegmentInfo()->docCount;
        if (segmentStatus == Segment::SegmentStatus::ST_BUILT) {
            indexers.emplace_back(segId, nullptr, segmentStatus, docCount);
            buildInfoHolder->diskSegments.emplace(segId, segment.get());
            continue;
        }
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        indexers.emplace_back(segId, indexer, segmentStatus, docCount);
    }
    return InitFromIndexers(indexers, buildInfoHolder);
}

Status DeletionMapIndexerOrganizerUtil::InitFromIndexers(
    const std::vector<std::tuple<segmentid_t, std::shared_ptr<indexlibv2::index::IIndexer>,
                                 indexlibv2::framework::Segment::SegmentStatus, size_t>>& indexers,
    SingleDeletionMapBuildInfoHolder* buildInfoHolder)
{
    docid_t lastBaseDocid = 0;
    for (auto& [segId, indexer, status, docCount] : indexers) {
        switch (status) {
        case Segment::SegmentStatus::ST_BUILT: {
            std::shared_ptr<DeletionMapDiskIndexer> diskIndexer =
                std::dynamic_pointer_cast<DeletionMapDiskIndexer>(indexer);
            buildInfoHolder->diskIndexers.push_back({segId, diskIndexer});
            break;
        }
        case Segment::SegmentStatus::ST_DUMPING: {
            std::shared_ptr<DeletionMapMemIndexer> memIndexer =
                std::dynamic_pointer_cast<DeletionMapMemIndexer>(indexer);
            assert(memIndexer != nullptr);
            buildInfoHolder->dumpingIndexers.push_back({segId, memIndexer});
            break;
        }
        case Segment::SegmentStatus::ST_BUILDING: {
            std::shared_ptr<DeletionMapMemIndexer> memIndexer =
                std::dynamic_pointer_cast<DeletionMapMemIndexer>(indexer);
            assert(memIndexer != nullptr);
            buildInfoHolder->buildingIndexer = memIndexer;
            break;
        }
        default:
            assert(false);
            break;
        }
        buildInfoHolder->segmentBaseDocids.push_back(lastBaseDocid);
        lastBaseDocid += docCount;
    }
    return Status::OK();
}

Status SingleDeletionMapBuildInfoHolder::MaybeLoadDiskIndexer(SingleDeletionMapBuildInfoHolder* buildInfoHolder,
                                                              size_t idx)
{
    segmentid_t segId = buildInfoHolder->diskIndexers[idx].first;
    auto diskIndexer = buildInfoHolder->diskIndexers[idx].second;
    if (diskIndexer != nullptr) {
        return Status::OK();
    }
    auto segmentIter = buildInfoHolder->diskSegments.find(segId);
    if (segmentIter == buildInfoHolder->diskSegments.end()) {
        return Status::InternalError("Segment not found: %d", segId);
    }
    indexlibv2::framework::Segment* segment = segmentIter->second;
    auto [status, indexer] = segment->GetIndexer(buildInfoHolder->deletionMapConfig->GetIndexType(),
                                                 buildInfoHolder->deletionMapConfig->GetIndexName());
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    buildInfoHolder->diskIndexers[idx].second = std::dynamic_pointer_cast<DeletionMapDiskIndexer>(indexer);
    return Status::OK();
}

Status SingleDeletionMapBuildInfoHolder::GetDiskIndexer(
    SingleDeletionMapBuildInfoHolder* buildInfoHolder, size_t idx,
    std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>* diskIndexer)
{
    RETURN_STATUS_DIRECTLY_IF_ERROR(MaybeLoadDiskIndexer(buildInfoHolder, idx));
    *diskIndexer = buildInfoHolder->diskIndexers[idx].second;
    return Status::OK();
}

} // namespace indexlib::index
