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
#ifndef __INDEXLIB_PARTITION_VERSION_H
#define __INDEXLIB_PARTITION_VERSION_H

#include <memory>
#include <set>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class PartitionVersion
{
public:
    PartitionVersion(const index_base::Version& version, segmentid_t buildingSegId, segmentid_t lastLinkRtSegId,
                     const std::set<segmentid_t>& dumpingSegmentIds);
    PartitionVersion(const index_base::Version& version, segmentid_t buildingSegId, segmentid_t lastLinkRtSegId);

    PartitionVersion(const PartitionVersion&) = delete;
    PartitionVersion(PartitionVersion&&) = delete;

    bool operator==(const PartitionVersion& other) const;
    bool operator!=(const PartitionVersion& other) const;

    segmentid_t GetLastLinkRtSegmentId() const;
    uint64_t GetHashId() const;
    bool IsUsingSegment(segmentid_t segId, TableType type) const;

private:
    index_base::Version mVersion;   // built segments
    segmentid_t mBuildingSegmentId; // building segment
    segmentid_t mLastLinkRtSegId;   // last linked realtime segment id (open in local storage)
    std::set<segmentid_t> mDumpingSegmentIds;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_VERSION_H
