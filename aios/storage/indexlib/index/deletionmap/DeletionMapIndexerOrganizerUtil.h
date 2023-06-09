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
#include "indexlib/framework/Segment.h"

namespace indexlibv2::framework {
class TabletData;
}
namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::index {
class DeletionMapDiskIndexer;
class DeletionMapMemIndexer;
class DeletionMapConfig;
class IIndexer;
} // namespace indexlibv2::index

namespace indexlib::index {

class SingleDeletionMapBuildInfoHolder
{
public:
    SingleDeletionMapBuildInfoHolder(fieldid_t _fieldId)
    {
        deletionMapConfig = nullptr;
        buildingIndexer = nullptr;
    }
    SingleDeletionMapBuildInfoHolder()
    {
        deletionMapConfig = nullptr;
        buildingIndexer = nullptr;
    }

public:
    std::shared_ptr<indexlibv2::index::DeletionMapConfig> deletionMapConfig;
    std::vector<std::pair<segmentid_t, std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>>> diskIndexers;
    std::map<segmentid_t, indexlibv2::framework::Segment*> diskSegments;
    std::vector<std::pair<segmentid_t, std::shared_ptr<indexlibv2::index::DeletionMapMemIndexer>>> dumpingIndexers;
    std::shared_ptr<indexlibv2::index::DeletionMapMemIndexer> buildingIndexer;
    std::vector<docid_t> segmentBaseDocids;

public:
    static Status GetDiskIndexer(SingleDeletionMapBuildInfoHolder* buildInfoHolder, size_t idx,
                                 std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>* diskIndexer);

private:
    static Status MaybeLoadDiskIndexer(SingleDeletionMapBuildInfoHolder* buildInfoHolder, size_t idx);
};

class DeletionMapIndexerOrganizerUtil
{
public:
    static Status Delete(docid_t docid, SingleDeletionMapBuildInfoHolder* buildInfoHolder);

    static Status InitFromTabletData(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                     const indexlibv2::framework::TabletData* tabletData,
                                     SingleDeletionMapBuildInfoHolder* buildInfoHolder);

private:
    static Status
    InitFromIndexers(const std::vector<std::tuple<segmentid_t, std::shared_ptr<indexlibv2::index::IIndexer>,
                                                  indexlibv2::framework::Segment::SegmentStatus, size_t>>& indexers,
                     SingleDeletionMapBuildInfoHolder* buildInfoHolder);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
