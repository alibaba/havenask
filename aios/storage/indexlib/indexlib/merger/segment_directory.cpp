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
#include "indexlib/merger/segment_directory.h"

#include <sstream>

#include "autil/StringUtil.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace fslib;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::file_system;

using namespace indexlib::util;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SegmentDirectory);

SegmentDirectory::SegmentDirectory()
{
    mSegInfoInited = false;
    mTotalDocCount = 0;
}

SegmentDirectory::SegmentDirectory(const file_system::DirectoryPtr& rootDir, const Version& version)
    : SegmentDirectoryBase(version)
    , mRootDir(rootDir)
    , mNewVersion(version)
{
    assert(mRootDir);
    mSegInfoInited = false;
    mTotalDocCount = 0;
    InitSegIdToSegmentMap();
}

SegmentDirectory::~SegmentDirectory() {}

void SegmentDirectory::Init(bool hasSub, bool needDeletionMap)
{
    DoInit(hasSub, needDeletionMap);
    if (hasSub) {
        mSubSegmentDirectory = CreateSubSegDir(needDeletionMap);
    }
}

void SegmentDirectory::DoInit(bool hasSub, bool needDeletionMap)
{
    InitSegIdToSegmentMap();
    InitPartitionData(hasSub, needDeletionMap);
}

file_system::DirectoryPtr SegmentDirectory::GetSegmentDirectory(segmentid_t segId) const
{
    Segments::const_iterator it = mSegments.find(segId);
    if (it != mSegments.end()) {
        return it->second.segDirectory;
    }
    return file_system::DirectoryPtr();
}

file_system::DirectoryPtr SegmentDirectory::GetRootDir() const { return mRootDir; }

file_system::DirectoryPtr SegmentDirectory::GetFirstValidRootDir() const { return mRootDir; }

void SegmentDirectory::InitSegmentInfo()
{
    if (mSegInfoInited) {
        return;
    }

    assert(mVersion.GetSegmentCount() == mSegments.size());

    mTotalDocCount = 0;
    Segments::const_iterator it = mSegments.begin();
    for (; it != mSegments.end(); ++it) {
        SegmentInfo segInfo;
        auto segDirectory = (it->second).segDirectory;
        if (!segInfo.Load(segDirectory)) {
            INDEXLIB_FATAL_ERROR(FileIO, "Load segment meta file failed, path[%s/%s]",
                                 segDirectory->DebugString().c_str(), SEGMENT_INFO_FILE_NAME.c_str());
        }
        segmentid_t segId = it->first;
        mSegmentInfos[segId] = segInfo;
        mBaseDocIds.push_back(mTotalDocCount);
        mTotalDocCount += segInfo.docCount;
    }
    mSegInfoInited = true;
}

void SegmentDirectory::InitSegIdToSegmentMap()
{
    Version::Iterator iter = mVersion.CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segId = iter.Next();
        string segDirName = mVersion.GetSegmentDirName(segId);
        file_system::DirectoryPtr segDirectory = mRootDir->GetDirectory(segDirName, false);
        if (segDirectory == nullptr) {
            INDEXLIB_FATAL_ERROR(NonExist, "direcroy not exist, path[%s/%s]", mRootDir->DebugString().c_str(),
                                 segDirName.c_str());
        }

        Segment segment(segDirectory, 0, segId);
        mSegments.insert(std::make_pair(segId, segment));
    }
}

void SegmentDirectory::GetBaseDocIds(std::vector<exdocid_t>& baseDocIds)
{
    if (!mSegInfoInited) {
        InitSegmentInfo();
    }

    baseDocIds = mBaseDocIds;
}

uint64_t SegmentDirectory::GetTotalDocCount()
{
    if (!mSegInfoInited) {
        InitSegmentInfo();
    }
    return mTotalDocCount;
}

bool SegmentDirectory::GetSegmentInfo(segmentid_t segId, SegmentInfo& segInfo)
{
    if (!mSegInfoInited) {
        InitSegmentInfo();
    }
    SegmentInfoMap::const_iterator it = mSegmentInfos.find(segId);
    if (it == mSegmentInfos.end()) {
        return false;
    }
    segInfo = it->second;
    return true;
}

uint32_t SegmentDirectory::GetDocCount(segmentid_t segId)
{
    if (!mSegInfoInited) {
        InitSegmentInfo();
    }
    SegmentInfoMap::const_iterator it = mSegmentInfos.find(segId);
    if (it == mSegmentInfos.end()) {
        return 0;
    }
    return it->second.docCount;
}

bool SegmentDirectory::LoadSegmentInfo(const file_system::DirectoryPtr& segDirectory, SegmentInfo& segInfo)
{
    return segInfo.Load(segDirectory);
}

SegmentDirectoryPtr SegmentDirectory::CreateSubSegDir(bool needDeletionMap) const
{
    SegmentDirectoryPtr segDir(Clone());
    Segments subSegments;
    const Segments& mainSegments = segDir->GetSegments();
    Segments::const_iterator it = mainSegments.begin();
    for (; it != mainSegments.end(); ++it) {
        Segment subSegment = it->second;
        subSegment.segDirectory = subSegment.segDirectory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        if (subSegment.segDirectory == nullptr) {
            INDEXLIB_FATAL_ERROR(NonExist, "directory not exist, path[%s/%s]",
                                 subSegment.segDirectory->DebugString().c_str(), SUB_SEGMENT_DIR_NAME);
        }
        subSegments.insert(std::make_pair(it->first, subSegment));
    }
    segDir->SetSegments(subSegments);
    // TODO:
    if (mPartitionData) {
        segDir->SetPartitionData(mPartitionData->GetSubPartitionData());
        if (needDeletionMap) {
            segDir->mDeletionMapReader.reset(new DeletionMapReader);
            segDir->mDeletionMapReader->Open(mPartitionData->GetSubPartitionData().get());
        }
    }
    return segDir;
}

SegmentDirectory* SegmentDirectory::Clone() const { return new SegmentDirectory(*this); }

segmentid_t SegmentDirectory::GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition,
                                                  segmentid_t physicalSegId) const
{
    return physicalSegId;
}

void SegmentDirectory::InitPartitionData(bool hasSub, bool needDeletionMap)
{
    mPartitionData = merger::MergePartitionDataCreator::CreateMergePartitionData(mRootDir, mVersion, hasSub);
    if (needDeletionMap) {
        mDeletionMapReader.reset(new DeletionMapReader);
        mDeletionMapReader->Open(mPartitionData.get());
    }
}

index::DeletionMapReaderPtr SegmentDirectory::GetDeletionMapReader() const { return mDeletionMapReader; }

void SegmentDirectory::RemoveSegment(segmentid_t segmentId)
{
    mNewVersion.RemoveSegment(segmentId);
    if (mNewVersion.GetVersionId() == mVersion.GetVersionId()) {
        mNewVersion.IncVersionId();
    }
}

void SegmentDirectory::AddSegmentTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta)
{
    mNewVersion.AddSegTemperatureMeta(temperatureMeta);
}

file_system::DirectoryPtr SegmentDirectory::CreateNewSegment(SegmentInfo& segInfo)
{
    file_system::DirectoryPtr segDir;
    segmentid_t newSegmentId = 0;
    segmentid_t lastSegId = mVersion.GetLastSegment();
    if (lastSegId != INVALID_SEGMENTID) {
        segInfo.mergedSegment = false;
        segInfo.docCount = 0;
        segInfo.maxTTL = 0;

        SegmentInfo lastSegInfo;
        if (GetSegmentInfo(lastSegId, lastSegInfo)) {
            segInfo.SetLocator(lastSegInfo.GetLocator());
            segInfo.timestamp = lastSegInfo.timestamp;
        } else {
            indexlibv2::framework::Locator locator;
            locator.Deserialize(mVersion.GetLocator().ToString());
            segInfo.SetLocator(locator);
            segInfo.timestamp = mVersion.GetTimestamp();
        }
        newSegmentId = lastSegId + 1;
    }
    string segDirName = mVersion.GetNewSegmentDirName(newSegmentId);
    if (!mPartitionData) {
        return segDir;
    }
    file_system::DirectoryPtr partitionDir = mPartitionData->GetRootDirectory();

    if (!partitionDir) {
        return segDir;
    }
    segDir = partitionDir->MakeDirectory(segDirName);
    if (!segDir) {
        return segDir;
    }
    if (mSubSegmentDirectory) {
        segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    }
    mNewVersion = mVersion;
    mNewVersion.AddSegment(newSegmentId);
    mNewVersion.IncVersionId();
    return segDir;
}

void SegmentDirectory::CommitVersion()
{
    if (mNewVersion == mVersion) {
        return;
    }

    VersionCommitter::DumpPartitionPatchMeta(GetRootDir(), mNewVersion);
    DeployIndexWrapper::DumpDeployMeta(GetRootDir(), mNewVersion);
    mNewVersion.Store(GetRootDir(), false);
    IndexSummary ret = IndexSummary::Load(GetRootDir(), mNewVersion, mVersion.GetVersionId());
    ret.Commit(GetRootDir());
    mVersion = mNewVersion;
    Reload();
}

void SegmentDirectory::Reload()
{
    bool hasSub = mSubSegmentDirectory.operator bool();
    bool needDeletionMap = GetDeletionMapReader().operator bool();
    Reset();
    Init(hasSub, needDeletionMap);
}

void SegmentDirectory::Reset()
{
    mNewVersion = mVersion;
    mSegments.clear();
    mSegmentInfos.clear();
    mBaseDocIds.clear();
    mTotalDocCount = 0;
    mSegInfoInited = false;
    mPartitionData.reset();
    mDeletionMapReader.reset();
    if (mSubSegmentDirectory) {
        mSubSegmentDirectory->Reset();
    }
    mSubSegmentDirectory.reset();
}

void SegmentDirectory::RemoveUselessSegment(const file_system::DirectoryPtr& rootDir)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    segmentid_t lastSegmentId = version.GetLastSegment();
    fslib::FileList segmentList;
    VersionLoader::ListSegments(rootDir, segmentList);
    for (size_t i = 0; i < segmentList.size(); ++i) {
        if (Version::GetSegmentIdByDirName(segmentList[i]) > lastSegmentId) {
            rootDir->RemoveDirectory(segmentList[i]);
        }
    }
}

string SegmentDirectory::GetCounterMapContent(const Segment& segment) const
{
    string counterMapContent = CounterMap::EMPTY_COUNTER_MAP_JSON;
    try {
        auto dir = segment.segDirectory;
        dir->LoadMayNonExist(COUNTER_FILE_NAME, counterMapContent);
    } catch (...) {
        throw;
    }
    return counterMapContent;
}

string SegmentDirectory::GetLatestCounterMapContent() const
{
    if (mSegments.empty()) {
        return CounterMap::EMPTY_COUNTER_MAP_JSON;
    }
    return GetCounterMapContent(mSegments.rbegin()->second);
}

vector<segmentid_t> SegmentDirectory::GetNewSegmentIds() const
{
    Version diff = mNewVersion - mVersion;
    return diff.GetSegmentVector();
}
}} // namespace indexlib::merger
