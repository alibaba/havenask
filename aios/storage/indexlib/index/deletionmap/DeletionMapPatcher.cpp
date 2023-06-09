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
#include "indexlib/index/deletionmap/DeletionMapPatcher.h"

#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapPatcher);

DeletionMapPatcher::DeletionMapPatcher() {}

DeletionMapPatcher::~DeletionMapPatcher() {}

Status
DeletionMapPatcher::LoadPatch(const std::vector<std::pair<std::shared_ptr<IIndexer>, segmentid_t>>& sourceIndexers,
                              const std::vector<segmentid_t>& targetSegmentIds, bool applyInBranch,
                              DeletionMapModifier* modifier)
{
    std::map<segmentid_t, std::shared_ptr<DeletionMapDiskIndexer>> sourceIndexerMap;
    for (size_t i = 0; i < sourceIndexers.size(); ++i) {
        auto diskIndexer = std::dynamic_pointer_cast<DeletionMapDiskIndexer>(sourceIndexers[i].first);
        auto segId = sourceIndexers[i].second;
        if (!diskIndexer) {
            AUTIL_LOG(ERROR, "get deletion map disk indexer from segment [%d] failed", segId);
            return Status::InvalidArgs("get deletion map indexer failed");
        }
        sourceIndexerMap[segId] = diskIndexer;
    }

    for (segmentid_t targetSegmentId : targetSegmentIds) {
        for (auto sourceIter = sourceIndexerMap.rbegin(); sourceIter != sourceIndexerMap.rend(); ++sourceIter) {
            segmentid_t sourceSegId = sourceIter->first;
            auto sourceIndexer = sourceIter->second;
            if (sourceSegId <= targetSegmentId) {
                break;
            }
            auto [status, patch] = sourceIndexer->GetDeletionMapPatch(targetSegmentId);
            RETURN_IF_STATUS_ERROR(status, "get deletion map patch from segment [%d] to segment [%d] failed",
                                   sourceSegId, targetSegmentId);
            if (patch) {
                AUTIL_LOG(INFO, "apply deletion patch from segment [%d] to segment [%d], apply in branch [%d]",
                          sourceSegId, targetSegmentId, applyInBranch);
                if (sourceIndexerMap.find(targetSegmentId) == sourceIndexerMap.end() && applyInBranch) {
                    RETURN_IF_STATUS_ERROR(modifier->ApplyPatchInBranch(targetSegmentId, patch.get()),
                                           "apply patch from segment [%d] to segment [%d] failed", sourceSegId,
                                           targetSegmentId);
                    break;
                } else {
                    RETURN_IF_STATUS_ERROR(modifier->ApplyPatch(targetSegmentId, patch.get()),
                                           "apply patch from segment [%d] to segment [%d] failed", sourceSegId,
                                           targetSegmentId);
                    break;
                }
            }
        }
    }

    return Status::OK();
}

} // namespace indexlibv2::index
