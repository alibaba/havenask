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
#include "indexlib/index_base/segment/multi_part_segment_directory.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MultiPartSegmentDirectory);

MultiPartSegmentDirectory::MultiPartSegmentDirectory() {}

MultiPartSegmentDirectory::MultiPartSegmentDirectory(const MultiPartSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEndSegmentIds(other.mEndSegmentIds)
{
    mSegmentDirectoryVec.clear();
    for (size_t i = 0; i < other.mSegmentDirectoryVec.size(); ++i) {
        SegmentDirectoryPtr segDir(other.mSegmentDirectoryVec[i]->Clone());
        mSegmentDirectoryVec.push_back(segDir);
    }
}

MultiPartSegmentDirectory::~MultiPartSegmentDirectory() {}

SegmentDirectory* MultiPartSegmentDirectory::Clone() { return new MultiPartSegmentDirectory(*this); }

void MultiPartSegmentDirectory::Init(const file_system::DirectoryVector& directoryVec, bool hasSub)
{
    vector<Version> versions;
    for (size_t i = 0; i < directoryVec.size(); ++i) {
        Version version;
        VersionLoader::GetVersion(directoryVec[i], version, INVALID_VERSIONID);
        versions.push_back(version);
    }
    Init(directoryVec, versions, hasSub);
}

void MultiPartSegmentDirectory::Init(const file_system::DirectoryVector& directoryVec,
                                     const std::vector<index_base::Version>& versions, bool hasSub)
{
    assert(directoryVec.size() == versions.size());
    segmentid_t virtualSegId = 0;
    for (size_t i = 0; i < directoryVec.size(); ++i) {
        SegmentDirectoryPtr segDir(new SegmentDirectory);
        segDir->Init(directoryVec[i], versions[i]);
        mSegmentDirectoryVec.push_back(segDir);

        Version version = segDir->GetVersion();
        for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
            mVersion.AddSegment(virtualSegId++);
        }
        mEndSegmentIds.push_back(virtualSegId);
    }

    InitSegmentDatas(mVersion);
    if (!versions.empty()) {
        mVersion.SetTimestamp(versions.rbegin()->GetTimestamp());
        mVersion.SetLocator(versions.rbegin()->GetLocator());
    }
    mOnDiskVersion = mVersion;

    if (mSegmentDirectoryVec.size() > 0) {
        // TODO: use first seg dir to init global member var
        SegmentDirectoryPtr segDir = mSegmentDirectoryVec[0];
        mRootDirectory = segDir->GetRootDirectory();
    }
    if (hasSub) {
        mSubSegmentDirectory.reset(Clone());
        mSubSegmentDirectory->SetSubSegmentDir();
    }
}

file_system::DirectoryPtr MultiPartSegmentDirectory::GetSegmentParentDirectory(segmentid_t segId) const
{
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    assert(segDir);
    return segDir->GetRootDirectory();
}

segmentid_t MultiPartSegmentDirectory::EncodeToVirtualSegmentId(size_t partitionIdx, segmentid_t phySegId) const
{
    if (partitionIdx >= mEndSegmentIds.size()) {
        return INVALID_SEGMENTID;
    }

    segmentid_t startSegmentId = 0;
    if (partitionIdx != 0) {
        startSegmentId = mEndSegmentIds[partitionIdx - 1];
    }

    const Version& version = mSegmentDirectoryVec[partitionIdx]->GetVersion();
    for (size_t i = 0; i < version.GetSegmentCount(); i++) {
        segmentid_t segId = version[i];
        if (phySegId == segId) {
            return startSegmentId + (segmentid_t)i;
        }
    }
    return INVALID_SEGMENTID;
}

bool MultiPartSegmentDirectory::DecodeVirtualSegmentId(segmentid_t virtualSegId, segmentid_t& physicalSegId,
                                                       size_t& partitionId) const
{
    assert(mEndSegmentIds.size() == mSegmentDirectoryVec.size());
    SegmentIdVector::const_iterator it = std::upper_bound(mEndSegmentIds.begin(), mEndSegmentIds.end(), virtualSegId);
    if (it == mEndSegmentIds.end()) {
        return false;
    }

    partitionId = it - mEndSegmentIds.begin();
    Version version = mSegmentDirectoryVec[partitionId]->GetVersion();
    size_t idx = virtualSegId;
    if (partitionId > 0) {
        idx = virtualSegId - mEndSegmentIds[partitionId - 1];
    }
    assert(idx < version.GetSegmentCount());
    physicalSegId = version[idx];
    return true;
}

SegmentDirectoryPtr MultiPartSegmentDirectory::GetSegmentDirectory(segmentid_t segId) const
{
    size_t partitionId = 0;
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    if (!DecodeVirtualSegmentId(segId, physicalSegId, partitionId)) {
        return SegmentDirectoryPtr();
    }
    assert(partitionId < mSegmentDirectoryVec.size());
    return mSegmentDirectoryVec[partitionId];
}

file_system::DirectoryPtr MultiPartSegmentDirectory::GetSegmentFsDirectory(segmentid_t segId) const
{
    size_t partitionId = 0;
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    if (!DecodeVirtualSegmentId(segId, physicalSegId, partitionId)) {
        return DirectoryPtr();
    }

    file_system::DirectoryPtr segmentParentDirectory = GetSegmentParentDirectory(segId);
    if (!segmentParentDirectory) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get segment parent dir, segId [%d]", segId);
    }
    SegmentDirectoryPtr segmentDirectory = GetSegmentDirectory(segId);
    assert(segmentDirectory);
    string segDirName = segmentDirectory->GetVersion().GetSegmentDirName(physicalSegId);
    file_system::DirectoryPtr segDirectory = segmentParentDirectory->GetDirectory(segDirName, false);

    IndexFileList segmentFileList;
    if (SegmentFileListWrapper::Load(segDirectory, segmentFileList)) {
        segDirectory->SetLifecycle(segmentFileList.lifecycle);
    } else {
        IE_LOG(ERROR, "can not find segment file list in [%s]", segDirName.c_str());
    }

    if (mIsSubSegDir) {
        if (!segDirectory) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get sub segment, dir [%s] in root [%s]", segDirName.c_str(),
                                 segmentParentDirectory->DebugString().c_str());
        }
        segDirectory = segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
    }
    if (segDirectory) {
        segDirectory->MountPackage();
    }
    return segDirectory;
}

void MultiPartSegmentDirectory::SetSubSegmentDir()
{
    SegmentDirectory::SetSubSegmentDir();
    for (size_t i = 0; i < mSegmentDirectoryVec.size(); ++i) {
        mSegmentDirectoryVec[i]->SetSubSegmentDir();
    }
}

const IndexFormatVersion& MultiPartSegmentDirectory::GetIndexFormatVersion() const
{
    if (mSegmentDirectoryVec.empty()) {
        static IndexFormatVersion formatVersion;
        return formatVersion;
    }
    return mSegmentDirectoryVec[0]->GetIndexFormatVersion();
}
}} // namespace indexlib::index_base
