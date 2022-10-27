#include "indexlib/partition/unused_segment_collector.h"
#include "indexlib/index_base/version_loader.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, UnusedSegmentCollector);

UnusedSegmentCollector::UnusedSegmentCollector() 
{
}

UnusedSegmentCollector::~UnusedSegmentCollector() 
{
}

void UnusedSegmentCollector::Collect(
        const ReaderContainerPtr& readerContainer,
        const DirectoryPtr& directory, FileList& segments, bool localOnly)
{
    Version latestVersion;
    VersionLoader::GetVersion(directory, latestVersion, INVALID_VERSION);
    fslib::FileList tmpSegments;
    VersionLoader::ListSegments(directory, tmpSegments, localOnly);
    for (size_t i = 0; i < tmpSegments.size(); ++i)
    {
        segmentid_t segId = Version::GetSegmentIdByDirName(
                tmpSegments[i]);
        if (segId > latestVersion.GetLastSegment())
        {
            continue;
        }
        if (latestVersion.HasSegment(segId))
        {
            continue;
        }
        if (readerContainer->IsUsingSegment(segId))
        {
            continue;
        }
        segments.push_back(tmpSegments[i]);
    }
}

IE_NAMESPACE_END(partition);

