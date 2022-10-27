#include "indexlib/partition/segment/segment_data_creator.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/build_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SegmentDataCreator);

SegmentDataCreator::SegmentDataCreator() 
{
}

SegmentDataCreator::~SegmentDataCreator() 
{
}


BuildingSegmentData SegmentDataCreator::CreateNewSegmentData(
    const SegmentDirectoryPtr& segDir,
    const InMemorySegmentPtr& lastDumpingSegment,
    const BuildConfig& buildConfig)
{
    if (!lastDumpingSegment)
    {
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
    newSegmentInfo.locator = lastSegInfo->locator;
    newSegmentInfo.timestamp = lastSegInfo->timestamp;
    newSegmentData.SetSegmentInfo(newSegmentInfo);
    return newSegmentData;
}
IE_NAMESPACE_END(partition);

