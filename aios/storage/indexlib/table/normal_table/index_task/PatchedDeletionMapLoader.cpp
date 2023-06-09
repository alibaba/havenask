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
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"
#include "indexlib/index/deletionmap/DeletionMapPatcher.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, PatchedDeletionMapLoader);

Status PatchedDeletionMapLoader::GetPatchedDeletionMapDiskIndexers(
    const std::shared_ptr<framework::TabletData>& tabletData,
    const std::shared_ptr<indexlib::file_system::Directory>& patchDir,
    std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>>* indexPairs)
{
    AUTIL_LOG(INFO, "begin GetPatchedDeletionMapDiskIndexers");
    assert(indexPairs);
    indexPairs->clear();
    assert(tabletData);
    // collect all indexer from tablet
    std::vector<std::pair<std::shared_ptr<index::IIndexer>, segmentid_t>> nonTypedIndexers;
    std::vector<segmentid_t> allSegmentIds;
    auto slice = tabletData->CreateSlice();
    for (auto segmentPtr : slice) {
        auto [status, indexer] =
            segmentPtr->GetIndexer(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_NAME);
        RETURN_IF_STATUS_ERROR(status, "get indexer failed");
        auto typedIndexer = std::dynamic_pointer_cast<index::DeletionMapDiskIndexer>(indexer);
        if (!typedIndexer) {
            AUTIL_LOG(ERROR, "cast to deletion map disk indexer failed");
            return Status::InvalidArgs("cast deletion map disk indexer failed");
        }
        indexPairs->emplace_back(segmentPtr->GetSegmentId(), typedIndexer);
        nonTypedIndexers.emplace_back(indexer, segmentPtr->GetSegmentId());
        allSegmentIds.emplace_back(segmentPtr->GetSegmentId());
    }

    // load patch between indexers
    auto deletionMapIndexConfig = tabletData->GetWriteSchema()->GetIndexConfig(index::DELETION_MAP_INDEX_TYPE_STR,
                                                                               index::DELETION_MAP_INDEX_NAME);

    index::DeletionMapModifier modifier;
    RETURN_IF_STATUS_ERROR(modifier.Open(deletionMapIndexConfig, tabletData.get()), "open deletionmap modifier failed");

    RETURN_IF_STATUS_ERROR(index::DeletionMapPatcher::LoadPatch(nonTypedIndexers, allSegmentIds,
                                                                /*applyInBranch*/ false, &modifier),
                           "load deletion map patch failed");
    AUTIL_LOG(INFO, "load patch between segment success");
    if (!patchDir) {
        AUTIL_LOG(INFO, "patchdir is nullptr, no need apply patch from extra patch dir");
        return Status::OK();
    }

    // load patch from op log patch dir
    std::shared_ptr<index::DeletionMapConfig> config(new index::DeletionMapConfig);
    index::DeletionMapDiskIndexer diskIndexer(0, INVALID_SEGMENTID);
    RETURN_IF_STATUS_ERROR(diskIndexer.Open(config, (patchDir ? patchDir->GetIDirectory() : nullptr)),
                           "open deletion map indexer from op patch dir failed");
    for (auto& [segmentid, indexer] : *indexPairs) {
        auto [status, bitmap] = diskIndexer.GetDeletionMapPatch(segmentid);
        RETURN_IF_STATUS_ERROR(status, "get deletion map patch for segment [%d] failed", segmentid);
        if (bitmap) {
            AUTIL_LOG(INFO, "begin load deletion map from patchdir for segment[%d]", segmentid);
            RETURN_IF_STATUS_ERROR(indexer->ApplyDeletionMapPatch(bitmap.get()),
                                   "apply deletion map patch for segment [%d] failed", segmentid);
        }
    }
    AUTIL_LOG(INFO, "load patch from op log patch dir success");
    return Status::OK();
}

} // namespace indexlibv2::table
