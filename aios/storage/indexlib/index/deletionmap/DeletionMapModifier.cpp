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
#include "indexlib/index/deletionmap/DeletionMapModifier.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapModifier);

DeletionMapModifier::DeletionMapModifier() {}

DeletionMapModifier::~DeletionMapModifier() {}

Status DeletionMapModifier::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                 const framework::TabletData* tabletData)
{
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitDeletionMapResource(tabletData));

    return indexlib::index::DeletionMapIndexerOrganizerUtil::InitFromTabletData(indexConfig, tabletData,
                                                                                &_buildInfoHolder);
}

Status DeletionMapModifier::InitDeletionMapResource(const framework::TabletData* tabletData)
{
    _deletionMapResources.clear();
    auto slice = tabletData->CreateSlice();
    auto resourceMap = tabletData->GetResourceMap();
    for (auto segment : slice) {
        segmentid_t segId = segment->GetSegmentId();
        auto docCount = segment->GetSegmentInfo()->docCount;
        auto resourceName = DeletionMapResource::GetResourceName(segId);
        auto deletionMapResource =
            std::dynamic_pointer_cast<DeletionMapResource>(resourceMap->GetResource(resourceName));
        if (!deletionMapResource) {
            deletionMapResource = DeletionMapResource::DeletionMapResourceCreator(docCount);
            RETURN_IF_STATUS_ERROR(resourceMap->AddSegmentResource(resourceName, segId, deletionMapResource),
                                   "add segment resource failed");
        }
        _deletionMapResources.push_back({segId, deletionMapResource});
    }
    return Status::OK();
}

Status DeletionMapModifier::ApplyPatchInBranch(segmentid_t targetSegmentId, indexlib::util::Bitmap* bitmap)
{
    for (auto& [segId, deletionMapResource] : _deletionMapResources) {
        if (segId == targetSegmentId) {
            bool ret = deletionMapResource->ApplyBitmap(bitmap);
            if (!ret) {
                assert(false);
                return Status::Corruption("bitmap apply failed for segment [%d]", targetSegmentId);
            }
            return Status::OK();
        }
    }
    return Status::NotFound("not find target segment [%d] deletionmap for patch", targetSegmentId);
}

Status DeletionMapModifier::ApplyPatch(segmentid_t targetSegmentId, indexlib::util::Bitmap* bitmap)
{
    for (size_t idx = 0; idx < _buildInfoHolder.diskIndexers.size(); idx++) {
        segmentid_t segId = _buildInfoHolder.diskIndexers[idx].first;
        if (segId == targetSegmentId) {
            std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer> diskIndexer = nullptr;
            RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::index::SingleDeletionMapBuildInfoHolder::GetDiskIndexer(
                &_buildInfoHolder, idx, &diskIndexer));
            return diskIndexer->ApplyDeletionMapPatch(bitmap);
        }
    }
    return Status::NotFound("not find target segment [%d] deletionmap for patch", targetSegmentId);
}

Status DeletionMapModifier::Delete(docid_t docid)
{
    return indexlib::index::DeletionMapIndexerOrganizerUtil::Delete(docid, &_buildInfoHolder);
}

Status DeletionMapModifier::DeleteInBranch(docid_t docid)
{
    const std::vector<docid_t>& segmentBaseDocids = _buildInfoHolder.segmentBaseDocids;
    size_t idx =
        std::upper_bound(segmentBaseDocids.begin(), segmentBaseDocids.end(), docid) - segmentBaseDocids.begin() - 1;
    docid_t inSegmentDocid = docid - segmentBaseDocids[idx];
    if (_deletionMapResources[idx].second->IsDeleted(inSegmentDocid) ||
        _deletionMapResources[idx].second->Delete(inSegmentDocid)) {
        return Status::OK();
    }
    AUTIL_LOG(ERROR, "delete docid [%d] failed, segment idx[%ld], base docid [%d]", docid, idx, segmentBaseDocids[idx]);
    return Status::Corruption("delete failed");
}

} // namespace indexlibv2::index
