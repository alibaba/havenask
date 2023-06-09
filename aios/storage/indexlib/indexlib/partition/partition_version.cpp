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
#include "indexlib/partition/partition_version.h"

#include "autil/StringUtil.h"
#include "indexlib/util/KeyHasherTyped.h"

using autil::StringUtil;
using std::string;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionVersion);

PartitionVersion::PartitionVersion(const index_base::Version& version, segmentid_t buildingSegId,
                                   segmentid_t lastLinkRtSegId)
    : mVersion(version)
    , mBuildingSegmentId(buildingSegId)
    , mLastLinkRtSegId(lastLinkRtSegId)
{
}

PartitionVersion::PartitionVersion(const index_base::Version& version, segmentid_t buildingSegId,
                                   segmentid_t lastLinkRtSegId, const std::set<segmentid_t>& dumpingSegmentIds)
    : mVersion(version)
    , mBuildingSegmentId(buildingSegId)
    , mLastLinkRtSegId(lastLinkRtSegId)
    , mDumpingSegmentIds(dumpingSegmentIds)
{
}

bool PartitionVersion::operator==(const PartitionVersion& other) const
{
    return mVersion == other.mVersion && mBuildingSegmentId == other.mBuildingSegmentId &&
           mLastLinkRtSegId == other.mLastLinkRtSegId && mDumpingSegmentIds == other.mDumpingSegmentIds;
}

bool PartitionVersion::operator!=(const PartitionVersion& other) const { return !(*this == other); }

segmentid_t PartitionVersion::GetLastLinkRtSegmentId() const { return mLastLinkRtSegId; }

bool PartitionVersion::IsUsingSegment(segmentid_t segId, TableType type) const
{
    if (mVersion.HasSegment(segId)) {
        return true;
    }

    if (mDumpingSegmentIds.find(segId) != mDumpingSegmentIds.end()) {
        return true;
    }

    return mBuildingSegmentId == segId;
}

uint64_t PartitionVersion::GetHashId() const
{
    string str =
        mVersion.ToString() + StringUtil::toString(mBuildingSegmentId) + StringUtil::toString(mLastLinkRtSegId);

    uint64_t hashKey = 0;
    util::MurmurHasher::GetHashKey(str.c_str(), str.size(), hashKey);
    return hashKey;
}
}} // namespace indexlib::partition
