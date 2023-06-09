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
#include "indexlib/index/deletionmap/DeletionMapMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapMerger);

DeletionMapMerger::DeletionMapMerger() : _hasCollected(false) {}

DeletionMapMerger::~DeletionMapMerger() {}

Status DeletionMapMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               const std::map<std::string, std::any>& params)
{
    return Status::OK();
}
Status DeletionMapMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                                const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    AUTIL_LOG(INFO, "deletion map merge begin");
    // check args
    if (!_hasCollected) {
        AUTIL_LOG(ERROR, "need collect deletion map before merge");
        return Status::InvalidArgs("need collect deletion map before merge");
    }
    if (segMergeInfos.targetSegments.empty()) {
        AUTIL_LOG(ERROR, "target segment is empty");
        return Status::InvalidArgs("target segment is empty");
    }

    for (const auto& segmentMeta : segMergeInfos.targetSegments) {
        if (!segmentMeta) {
            AUTIL_LOG(ERROR, "target segment meta is nullptr");
            return Status::InvalidArgs("segment meta is nullptr");
        }
        auto targetSegmentDir = segmentMeta->segmentDir;
        if (!targetSegmentDir) {
            AUTIL_LOG(ERROR, "target segment dir is empty");
            return Status::InvalidArgs("target segment dir is empty");
        }

        indexlib::file_system::DirectoryOption dirOption;
        auto targetDirResult = targetSegmentDir->GetIDirectory()->MakeDirectory(DELETION_MAP_INDEX_PATH, dirOption);
        if (!targetDirResult.OK()) {
            AUTIL_LOG(ERROR, "make target directory [%s] failed", DELETION_MAP_INDEX_PATH.c_str());
            return Status::InvalidArgs("make target dir failed");
        }
    }
    std::shared_ptr<framework::SegmentMeta> targetSegmentMeta = *segMergeInfos.targetSegments.rbegin();
    if (!targetSegmentMeta) {
        AUTIL_LOG(ERROR, "target segment meta is nullptr");
        return Status::InvalidArgs("segment meta is nullptr");
    }
    auto targetSegmentDir = targetSegmentMeta->segmentDir;
    if (!targetSegmentDir) {
        AUTIL_LOG(ERROR, "target segment dir is empty");
        return Status::InvalidArgs("target segment dir is empty");
    }

    auto targetSegmentId = targetSegmentMeta->segmentId;

    indexlib::file_system::DirectoryOption dirOption;
    auto targetDirResult = targetSegmentDir->GetIDirectory()->MakeDirectory(DELETION_MAP_INDEX_PATH, dirOption);
    if (!targetDirResult.OK()) {
        AUTIL_LOG(ERROR, "make target directory [%s] failed", DELETION_MAP_INDEX_PATH.c_str());
        return Status::InvalidArgs("make target dir failed");
    }

    // target directory segment_x_level_0/deletionmap
    auto targetDir = targetDirResult.Value();

    std::set<segmentid_t> sourceSegmentIds;
    for (auto& segmentInfo : segMergeInfos.srcSegments) {
        sourceSegmentIds.insert(segmentInfo.segment->GetSegmentId());
    }
    // dump to last segment
    // and segment has not patch to bigger than it's segment
    for (auto& [segmentid, indexer] : _allIndexers) {
        if (sourceSegmentIds.find(segmentid) != sourceSegmentIds.end() || targetSegmentId < segmentid) {
            AUTIL_LOG(INFO, "ignore dump deletion map for segment[%d]", segmentid);
            continue;
        }
        AUTIL_LOG(INFO, "begin dump deletion map for segment[%d]", segmentid);
        RETURN_IF_STATUS_ERROR(indexer->Dump(targetDir), "dump segment [%d] failed", segmentid);
    }
    AUTIL_LOG(INFO, "deletion map merge success");
    return Status::OK();
}

void DeletionMapMerger::CollectAllDeletionMap(
    const std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>>>& patchedAllIndexers)
{
    _allIndexers = patchedAllIndexers;
    _hasCollected = true;
}

} // namespace indexlibv2::index
