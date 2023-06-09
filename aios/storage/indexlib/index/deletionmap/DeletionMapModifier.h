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

#include <memory>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/deletionmap/DeletionMapIndexerOrganizerUtil.h"
#include "indexlib/index/deletionmap/DeletionMapResource.h"
namespace indexlibv2::config {
class IIndexConfig;
}
namespace indexlibv2::framework {
class TabletData;
}
namespace indexlibv2::index {
class DeletionMapDiskIndexer;
class DeletionMapMemIndexer;
class IIndexer;

class DeletionMapModifier
{
public:
    DeletionMapModifier();
    ~DeletionMapModifier();

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig, const framework::TabletData* tabletData);
    Status Delete(docid_t docid);
    Status DeleteInBranch(docid_t docid);
    Status ApplyPatchInBranch(segmentid_t targetSegmentId, indexlib::util::Bitmap* bitmap);
    Status ApplyPatch(segmentid_t targetSegmentId, indexlib::util::Bitmap* bitmap);

private:
    Status InitDeletionMapResource(const framework::TabletData* tabletData);

private:
    // TODO(makuo.mnb) add global bitmap
    indexlib::index::SingleDeletionMapBuildInfoHolder _buildInfoHolder;
    std::vector<std::pair<segmentid_t, std::shared_ptr<DeletionMapResource>>> _deletionMapResources;

private:
    AUTIL_LOG_DECLARE();
    friend class DeletionMapIndexReaderTest;
};

} // namespace indexlibv2::index
