#include <autil/StringUtil.h>
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, OfflineRecoverStrategy);

OfflineRecoverStrategy::OfflineRecoverStrategy() 
{
}

OfflineRecoverStrategy::~OfflineRecoverStrategy() 
{
}

Version OfflineRecoverStrategy::Recover(
    Version version, const DirectoryPtr& physicalDir,
    RecoverStrategyType recoverType)
{
    IE_LOG(INFO, "Begin recover partition!");
    segmentid_t lastSegmentId = version.GetLastSegment();
    vector<SegmentInfoPair> lostSegments;
    GetLostSegments(physicalDir, version, lastSegmentId, lostSegments);

    IE_LOG(INFO, "Latest ondisk version [%s]", version.ToString().c_str());
    Version recoveredVersion = version;

    if (recoverType == RecoverStrategyType::RST_VERSION_LEVEL)
    {
        CleanUselessSegments(lostSegments, 0, physicalDir);
        IE_LOG(INFO, "Recovered version [%s]", recoveredVersion.ToString().c_str());
        IE_LOG(INFO, "End recover partition!"); 
        return recoveredVersion; 
    }
    for (size_t i = 0; i < lostSegments.size(); i++)
    {
        segmentid_t segId = lostSegments[i].first;
        string segDirName = lostSegments[i].second;
        DirectoryPtr segmentDirectory = physicalDir->GetDirectory(segDirName, true);
        assert(segmentDirectory);
 
        SegmentInfo segInfo;
        if (!segInfo.Load(segmentDirectory) || segInfo.mergedSegment)
        {
            CleanUselessSegments(lostSegments, i, physicalDir);
            break;
        }
        recoveredVersion.AddSegment(segId);
        recoveredVersion.SetTimestamp(segInfo.timestamp);
        recoveredVersion.SetLocator(segInfo.locator);
    }

    IE_LOG(INFO, "Recovered version [%s]", recoveredVersion.ToString().c_str());
    IE_LOG(INFO, "End recover partition!");
    return recoveredVersion;
}

void OfflineRecoverStrategy::RemoveLostSegments(
        Version version, const DirectoryPtr& physicalDir)
{
    IE_LOG(INFO, "Begin remove lost segments in [%s] for version [%d]!",
           physicalDir->GetPath().c_str(), version.GetVersionId());
    vector<SegmentInfoPair> lostSegments;
    GetLostSegments(physicalDir, version, version.GetLastSegment(), lostSegments);
    CleanUselessSegments(lostSegments, 0, physicalDir);
}

void OfflineRecoverStrategy::GetLostSegments(
        const DirectoryPtr& directory, const Version& version,
        segmentid_t lastSegmentId, 
        vector<SegmentInfoPair>& lostSegments)
{
    FileList fileList;
    bool skipSecondaryRoot = StringUtil::endsWith(directory->GetPath(), RT_INDEX_PARTITION_DIR_NAME)
                             || StringUtil::endsWith(directory->GetPath(), JOIN_INDEX_PARTITION_DIR_NAME);
    directory->ListFile("", fileList, false, false, skipSecondaryRoot);
    
    for (size_t i = 0; i < fileList.size(); i++)
    {
        if (!version.IsValidSegmentDirName(fileList[i]))
        {
            continue;
        }
        segmentid_t segId = Version::GetSegmentIdByDirName(fileList[i]);
        if (segId > lastSegmentId)
        {
            lostSegments.push_back({segId, fileList[i]});
        }
    }
    sort(lostSegments.begin(), lostSegments.end());
}

void OfflineRecoverStrategy::CleanUselessSegments(
        const vector<SegmentInfoPair>& lostSegments,
        size_t firstUselessSegIdx, 
        const DirectoryPtr& rootDirectory)
{
    for (size_t i = firstUselessSegIdx; i < lostSegments.size(); i++)
    {
        IE_LOG(INFO, "remove lost segment [%s]", lostSegments[i].second.c_str());
        rootDirectory->RemoveDirectory(lostSegments[i].second);
    }
}

IE_NAMESPACE_END(index_base);

