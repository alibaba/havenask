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
#include "indexlib/partition/segment/segment_data_creator.h"

#include "indexlib/config/build_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SegmentDataCreator);

SegmentDataCreator::SegmentDataCreator() {}

SegmentDataCreator::~SegmentDataCreator() {}

BuildingSegmentData SegmentDataCreator::CreateNewSegmentData(const SegmentDirectoryPtr& segDir,
                                                             const InMemorySegmentPtr& lastDumpingSegment,
                                                             const BuildConfig& buildConfig)
{
    if (!lastDumpingSegment) {
        return segDir->CreateNewSegmentData(buildConfig);
    }
    BuildingSegmentData newSegmentData(buildConfig);
    SegmentData lastSegData = lastDumpingSegment->GetSegmentData();
    SegmentInfoPtr lastSegInfo = lastDumpingSegment->GetSegmentInfo();
    newSegmentData.SetSegmentId(lastDumpingSegment->GetSegmentId() + 1);
    newSegmentData.SetBaseDocId(lastSegData.GetBaseDocId() + lastSegInfo->docCount);
    const Version& version = segDir->GetVersion();
    newSegmentData.SetSegmentDirName(version.GetNewSegmentDirName(newSegmentData.GetSegmentId()));
    SegmentInfo newSegmentInfo;
    newSegmentInfo.SetLocator(lastSegInfo->GetLocator());
    newSegmentInfo.timestamp = lastSegInfo->timestamp;
    newSegmentData.SetSegmentInfo(newSegmentInfo);
    return newSegmentData;
}
}} // namespace indexlib::partition
