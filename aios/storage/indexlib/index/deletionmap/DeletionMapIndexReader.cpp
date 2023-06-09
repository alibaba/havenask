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
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapMetrics.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapIndexReader);

std::pair<Status, std::unique_ptr<DeletionMapIndexReader>>
DeletionMapIndexReader::Create(const framework::TabletData* tabletData)
{
    if (unlikely(tabletData == nullptr)) {
        return {Status::InvalidArgs("can not find tabletData"), nullptr};
    }
    auto slice = tabletData->CreateSlice();
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segmentSchema = (*iter)->GetSegmentSchema();
        if (!segmentSchema) {
            continue;
        }
        auto indexConfig = segmentSchema->GetIndexConfig(DELETION_MAP_INDEX_TYPE_STR, DELETION_MAP_INDEX_NAME);
        if (indexConfig) {
            auto deletionMapReader = std::make_unique<DeletionMapIndexReader>();
            auto status = deletionMapReader->Open(indexConfig, tabletData);
            RETURN2_IF_STATUS_ERROR(status, nullptr, "open segment [%d] deletionmap reader failed",
                                    (*iter)->GetSegmentId());
            return {Status::OK(), std::move(deletionMapReader)};
        }
    }
    return {Status::OK(), nullptr};
}

DeletionMapIndexReader::DeletionMapIndexReader() {}
DeletionMapIndexReader::~DeletionMapIndexReader()
{
    if (_metrics) {
        _metrics->Stop();
    }
}

Status DeletionMapIndexReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                    const framework::TabletData* tabletData)
{
    std::vector<std::tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>> indexers;
    _deletionMapResources.clear();
    auto slice = tabletData->CreateSlice();
    auto resourceMap = tabletData->GetResourceMap();
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segmentStatus = (*iter)->GetSegmentStatus();
        segmentid_t segId = (*iter)->GetSegmentId();
        auto docCount = (*iter)->GetSegmentInfo()->docCount;
        auto indexerPair = (*iter)->GetIndexer(DELETION_MAP_INDEX_TYPE_STR, DELETION_MAP_INDEX_NAME);
        if (!indexerPair.first.IsOK()) {
            return indexerPair.first;
        }
        auto indexer = indexerPair.second;
        indexers.emplace_back(indexer, segmentStatus, docCount);
        auto resourceName = DeletionMapResource::GetResourceName(segId);
        auto deletionMapResource =
            std::dynamic_pointer_cast<DeletionMapResource>(resourceMap->GetResource(resourceName));
        if (!deletionMapResource) {
            deletionMapResource = DeletionMapResource::DeletionMapResourceCreator(docCount);
            RETURN_IF_STATUS_ERROR(resourceMap->AddSegmentResource(resourceName, segId, deletionMapResource),
                                   "add segment resource failed");
        }
        _deletionMapResources.push_back(deletionMapResource);
    }
    _tabletDataInfo = tabletData->GetTabletDataInfo();
    if (!_tabletDataInfo) {
        AUTIL_LOG(ERROR, "get tablet data info failed");
        return Status::Corruption("get tablet data info failed");
    }
    return DoOpen(indexers);
}

Status DeletionMapIndexReader::DoOpen(
    const std::vector<std::tuple<std::shared_ptr<IIndexer>, framework::Segment::SegmentStatus, size_t>>& indexers)
{
    docid_t lastBaseDocid = 0;
    for (auto& [indexer, status, docCount] : indexers) {
        switch (status) {
        case framework::Segment::SegmentStatus::ST_BUILT: {
            std::shared_ptr<DeletionMapDiskIndexer> diskIndexer =
                std::dynamic_pointer_cast<DeletionMapDiskIndexer>(indexer);
            assert(diskIndexer);
            _builtIndexers.push_back(diskIndexer);
            _segmentBaseDocids.push_back(lastBaseDocid);
            lastBaseDocid += docCount;
            break;
        }
        case framework::Segment::SegmentStatus::ST_DUMPING: {
            std::shared_ptr<DeletionMapMemIndexer> memIndexer =
                std::dynamic_pointer_cast<DeletionMapMemIndexer>(indexer);
            assert(memIndexer);
            _dumpingIndexers.push_back(memIndexer);
            _segmentBaseDocids.push_back(lastBaseDocid);
            lastBaseDocid += docCount;
            break;
        }
        case framework::Segment::SegmentStatus::ST_BUILDING: {
            std::shared_ptr<DeletionMapMemIndexer> memIndexer =
                std::dynamic_pointer_cast<DeletionMapMemIndexer>(indexer);
            _segmentBaseDocids.push_back(lastBaseDocid);
            assert(memIndexer);
            _buildingIndexer = memIndexer;
            break;
        }
        default:
            assert(false);
            break;
        }
    }
    if (!_buildingIndexer) {
        _segmentBaseDocids.push_back(lastBaseDocid);
    }
    if (_metrics) {
        _metrics->Start();
    }
    return Status::OK();
}

uint32_t DeletionMapIndexReader::GetDeletedDocCount() const
{
    uint32_t deletedCount = 0;
    for (auto& indexer : _builtIndexers) {
        deletedCount += indexer->GetDeletedDocCount();
    }
    for (auto& indexer : _dumpingIndexers) {
        deletedCount += indexer->GetDeletedDocCount();
    }
    if (_buildingIndexer) {
        deletedCount += _buildingIndexer->GetDeletedDocCount();
    }
    return deletedCount;
}

uint32_t DeletionMapIndexReader::GetSegmentDeletedDocCount(segmentid_t segmentId) const
{
    for (auto& indexer : _builtIndexers) {
        if (indexer->GetSegmentId() == segmentId) {
            return indexer->GetDeletedDocCount();
        }
    }
    for (auto& indexer : _dumpingIndexers) {
        if (indexer->GetSegmentId() == segmentId) {
            return indexer->GetDeletedDocCount();
        }
    }
    if (_buildingIndexer) {
        if (_buildingIndexer->GetSegmentId() == segmentId) {
            return _buildingIndexer->GetDeletedDocCount();
        }
    }
    return 0;
}

void DeletionMapIndexReader::RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics) { _metrics = metrics; }

} // namespace indexlibv2::index
