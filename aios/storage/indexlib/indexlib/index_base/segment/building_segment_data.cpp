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
#include "indexlib/index_base/segment/building_segment_data.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, BuildingSegmentData);

BuildingSegmentData::BuildingSegmentData() : mIsSubSegment(false) {}

BuildingSegmentData::BuildingSegmentData(const config::BuildConfig& buildConfig)
    : mIsSubSegment(false)
    , mBuildConfig(buildConfig)
{
}

BuildingSegmentData::~BuildingSegmentData() {}

BuildingSegmentData::BuildingSegmentData(const BuildingSegmentData& segData)
    : SegmentData(segData)
    , mIsSubSegment(segData.mIsSubSegment)
    , mPartitionDir(segData.mPartitionDir)
    , mBuildConfig(segData.mBuildConfig)
{
}

void BuildingSegmentData::Init(const DirectoryPtr& directory, bool isSubSegment)
{
    mPartitionDir = directory;
    mIsSubSegment = isSubSegment;
    if (mSubSegmentData) {
        BuildingSegmentDataPtr subSegData = DYNAMIC_POINTER_CAST(BuildingSegmentData, mSubSegmentData);
        assert(subSegData);
        subSegData->Init(directory, true);
    }
}

void BuildingSegmentData::PrepareDirectory()
{
    if (mDirectory) {
        return;
    }

    string segDirName = GetSegmentDirName();
    assert(!segDirName.empty());
    DirectoryPtr segDirectory;
    try {
        segDirectory = mPartitionDir->MakeDirectory(segDirName, DirectoryOption::PackageMem());
    } catch (const util::ExceptionBase& e) {
        IE_LOG(WARN, "Make PackageMem Directory for [%s] fail : reason [%s]", segDirName.c_str(), e.what());
        segDirectory = mPartitionDir->GetDirectory(segDirName, true);
    }
    if (!mIsSubSegment) {
        SetDirectory(segDirectory);
        return;
    }

    assert(mIsSubSegment);
    if (!segDirectory->IsExist(SUB_SEGMENT_DIR_NAME)) {
        segDirectory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    }
    SetDirectory(segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, true));
}

BuildingSegmentData BuildingSegmentData::CreateShardingSegmentData(uint32_t idx)
{
    if (mIsSubSegment) {
        INDEXLIB_FATAL_ERROR(UnSupported, "sub segment not support sharding column!");
    }

    if (!mDirectory) {
        PrepareDirectory();
    }
    assert(mDirectory);
    string dirName = SegmentData::GetShardingDirInSegment(idx);
    if (!mDirectory->IsExist(dirName)) {
        // TODO: test create in mem directory in offline
        mDirectory->MakeDirectory(dirName, DirectoryOption::Mem());
    }

    DirectoryPtr directory = mDirectory->GetDirectory(dirName, true);
    BuildingSegmentData segData(*this);
    segData.SetDirectory(directory);
    return segData;
}
}} // namespace indexlib::index_base
