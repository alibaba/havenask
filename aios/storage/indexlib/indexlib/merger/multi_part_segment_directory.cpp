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
#include "indexlib/merger/multi_part_segment_directory.h"

#include <map>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <utility>

#include "autil/legacy/exception.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/CounterBase.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace fslib;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MultiPartSegmentDirectory);

MultiPartSegmentDirectory::MultiPartSegmentDirectory(const vector<file_system::DirectoryPtr>& roots,
                                                     const vector<Version>& versions)
    : SegmentDirectory()
{
    assert(roots.size() == versions.size());
    mRootDirs = roots;
    mVersions = versions;
    mPartSegIds.resize(versions.size());
}

MultiPartSegmentDirectory::~MultiPartSegmentDirectory() {}

void MultiPartSegmentDirectory::DoInit(bool hasSub, bool needDeletionMap)
{
    segmentid_t virtualSegId = 0;
    mVersion = Version(0);
    for (auto& version : mVersions) {
        if (version.GetVersionId() == INVALID_VERSIONID) {
            continue;
        }
        const indexlibv2::framework::LevelInfo& srcLevelInfo = version.GetLevelInfo();
        if (!srcLevelInfo.levelMetas.empty()) {
            indexlibv2::framework::LevelInfo& levelInfo = mVersion.GetLevelInfo();
            levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(), srcLevelInfo.GetShardCount());
        }
        break;
    }

    for (size_t i = 0; i < mVersions.size(); ++i) {
        for (size_t j = 0; j < mVersions[i].GetSegmentCount(); ++j) {
            auto srcSegId = mVersions[i][j];
            InitOneSegment(i, srcSegId, virtualSegId, mVersions[i]);
            mVersion.AddSegment(virtualSegId);
            SegmentTemperatureMeta meta;
            if (mVersions[i].GetSegmentTemperatureMeta(srcSegId, meta)) {
                meta.segmentId = virtualSegId;
                mVersion.AddSegTemperatureMeta(meta);
            }
            indexlibv2::framework::SegmentStatistics segStats;
            if (mVersions[i].GetSegmentStatistics(srcSegId, &segStats)) {
                segStats.SetSegmentId(virtualSegId);
                mVersion.AddSegmentStatistics(segStats);
            }
            virtualSegId++;
        }
    }

    if (!mVersions.empty()) {
        ProgressSynchronizer ps;
        ps.Init(mVersions);
        mVersion.SetTimestamp(ps.GetTimestamp());
        mVersion.SetLocator(ps.GetLocator());
        mVersion.SetFormatVersion(ps.GetFormatVersion());
    }
    mPartitionData = merger::MergePartitionDataCreator::CreateMergePartitionData(mRootDirs, mVersions, hasSub);
    if (needDeletionMap) {
        mDeletionMapReader.reset(new DeletionMapReader);
        mDeletionMapReader->Open(mPartitionData.get());
    }
}

void MultiPartSegmentDirectory::InitOneSegment(partitionid_t partId, segmentid_t physicalSegId,
                                               segmentid_t virtualSegId, const Version& version)
{
    assert(partId < (partitionid_t)mPartSegIds.size());
    auto segDirectory = SegmentDirectoryFinder::MakeSegmentDirectory(mRootDirs[partId], physicalSegId, version);
    Segment segment(segDirectory, partId, physicalSegId);
    auto it = mSegments.find(virtualSegId);
    if (it != mSegments.end()) {
        it->second = segment;
    } else {
        mSegments.insert(std::make_pair(virtualSegId, segment));
    }
    mPartSegIds[partId].push_back(virtualSegId);
}

SegmentDirectory* MultiPartSegmentDirectory::Clone() const { return new MultiPartSegmentDirectory(*this); }

segmentid_t MultiPartSegmentDirectory::GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition,
                                                           segmentid_t physicalSegId) const
{
    Segments::const_iterator iter = mSegments.find(virtualSegIdInSamePartition);
    assert(iter != mSegments.end());
    partitionid_t partId = iter->second.partId;

    for (Segments::const_iterator it = mSegments.begin(); it != mSegments.end(); ++it) {
        if (it->second.physicalSegId == physicalSegId && it->second.partId == partId) {
            return it->first;
        }
    }
    return INVALID_SEGMENTID;
}

void MultiPartSegmentDirectory::GetPhysicalSegmentId(segmentid_t virtualSegId, segmentid_t& physicalSegId,
                                                     partitionid_t& partId) const
{
    Segments::const_iterator iter = mSegments.find(virtualSegId);
    assert(iter != mSegments.end());
    partId = iter->second.partId;
    physicalSegId = iter->second.physicalSegId;
}

void MultiPartSegmentDirectory::Reload(const vector<Version>& versions)
{
    assert(versions.size() == mVersions.size());
    bool hasSub = mSubSegmentDirectory.operator bool();
    bool needDeletionMap = GetDeletionMapReader().operator bool();
    mVersions = versions;
    SegmentDirectory::Reset();
    for (size_t i = 0; i < mPartSegIds.size(); ++i) {
        mPartSegIds[i].clear();
    }
    Init(hasSub, needDeletionMap);
}

string MultiPartSegmentDirectory::GetLatestCounterMapContent() const
{
    if (mPartSegIds.empty()) {
        return CounterMap::EMPTY_COUNTER_MAP_JSON;
    }

    CounterMap mergedCounterMap;
    for (const auto& segVec : mPartSegIds) {
        if (segVec.empty()) {
            continue;
        }
        segmentid_t lastSegIdOfPartition = segVec.back();
        mergedCounterMap.Merge(GetCounterMapContent(mSegments.at(lastSegIdOfPartition)), CounterBase::FJT_MERGE);
    }
    return mergedCounterMap.ToJsonString();
}
}} // namespace indexlib::merger
