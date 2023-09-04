#include "indexlib/test/version_maker.h"

#include "autil/StringUtil.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, VersionMaker);

VersionMaker::VersionMaker() {}

VersionMaker::~VersionMaker() {}

Version VersionMaker::Make(versionid_t versionId, const string& incSegmentIds, const string& rtSegmentIds,
                           const string& joinSegmentIds, timestamp_t ts)

{
    return Make(DirectoryPtr(), versionId, incSegmentIds, rtSegmentIds, joinSegmentIds, ts, false);
}

Version VersionMaker::Make(file_system::DirectoryPtr rootDirectory, versionid_t versionId,
                           const std::string& incSegmentIds, const std::string& rtSegmentIds,
                           const std::string& joinSegmentIds, timestamp_t ts, bool needSub)
{
    vector<segmentid_t> incSegmentIdVector;
    autil::StringUtil::fromString(incSegmentIds, incSegmentIdVector, ",");

    Version version;
    for (size_t i = 0; i < incSegmentIdVector.size(); ++i) {
        segmentid_t segId = MakeIncSegment(rootDirectory, incSegmentIdVector[i], needSub, ts);
        version.AddSegment(segId);
    }
    version.SetVersionId(versionId);
    indexlibv2::framework::Locator locator;
    version.SetLocator(document::Locator(locator.Serialize()));
    version.SetTimestamp(ts);
    if (rootDirectory && incSegmentIdVector.size() > 0) {
        version.Store(rootDirectory, true);
    }

    vector<segmentid_t> joinSegmentIdVector;
    autil::StringUtil::fromString(joinSegmentIds, joinSegmentIdVector, ",");

    Version joinVersion;
    for (size_t i = 0; i < joinSegmentIdVector.size(); ++i) {
        segmentid_t segId = MakeJoinSegment(rootDirectory, joinSegmentIdVector[i], needSub);
        joinVersion.AddSegment(segId);
        version.AddSegment(segId);
    }
    joinVersion.SetVersionId(versionId);
    joinVersion.SetTimestamp(ts);
    if (rootDirectory && joinSegmentIdVector.size() > 0) {
        joinVersion.Store(rootDirectory->GetDirectory(JOIN_INDEX_PARTITION_DIR_NAME, true), true);
    }

    vector<segmentid_t> rtSegmentIdVector;
    autil::StringUtil::fromString(rtSegmentIds, rtSegmentIdVector, ",");

    Version rtVersion;
    for (size_t i = 0; i < rtSegmentIdVector.size(); ++i) {
        segmentid_t segId = MakeRealtimeSegment(rootDirectory, rtSegmentIdVector[i], needSub, ts);
        rtVersion.AddSegment(segId);
        version.AddSegment(segId);
    }
    rtVersion.SetVersionId(versionId);
    rtVersion.SetTimestamp(ts);
    if (rootDirectory && rtSegmentIdVector.size() > 0) {
        rtVersion.Store(rootDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true), true);
    }
    if (rootDirectory) {
        rootDirectory->Sync(true);
    }
    return version;
}

segmentid_t VersionMaker::MakeIncSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId, bool needSub,
                                         int64_t ts)
{
    if (!rootDirectory) {
        return rawSegId;
    }
    Version version;
    string segDirName = version.GetNewSegmentDirName(rawSegId);
    if (rootDirectory->IsExist(segDirName)) {
        return rawSegId;
    }
    DirectoryPtr segDirectory = rootDirectory->MakeDirectory(segDirName, DirectoryOption::Mem());

    SegmentInfo segInfo;
    segInfo.timestamp = ts;
    segInfo.Store(segDirectory);
    CounterMap counterMap;
    segDirectory->Store(COUNTER_FILE_NAME, counterMap.ToJsonString(), WriterOption());

    if (needSub) {
        DirectoryPtr subSegDirectory = segDirectory->MakeDirectory(SUB_SEGMENT_DIR_NAME, DirectoryOption::Mem());
        segInfo.Store(subSegDirectory);
        subSegDirectory->Store(COUNTER_FILE_NAME, counterMap.ToJsonString(), WriterOption());
    }

    SegmentFileListWrapper::Dump(segDirectory, "");
    rootDirectory->Sync(true);
    return rawSegId;
}

segmentid_t VersionMaker::MakeRealtimeSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId,
                                              bool needSub, int64_t ts)
{
    segmentid_t rtSegmentId = index_base::RealtimeSegmentDirectory::ConvertToRtSegmentId(rawSegId);
    if (!rootDirectory) {
        return rtSegmentId;
    }

    DirectoryPtr rtDirectory;
    if (!rootDirectory->IsExist(RT_INDEX_PARTITION_DIR_NAME)) {
        rtDirectory = rootDirectory->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
    } else {
        rtDirectory = rootDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true);
    }

    MakeIncSegment(rtDirectory, rtSegmentId, needSub, ts);
    return rtSegmentId;
}

segmentid_t VersionMaker::MakeJoinSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId, bool needSub,
                                          int64_t ts)
{
    segmentid_t joinSegmentId = index_base::JoinSegmentDirectory::ConvertToJoinSegmentId(rawSegId);
    if (!rootDirectory) {
        return joinSegmentId;
    }

    DirectoryPtr joinDirectory;
    if (!rootDirectory->IsExist(JOIN_INDEX_PARTITION_DIR_NAME)) {
        joinDirectory = rootDirectory->MakeDirectory(JOIN_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
    } else {
        joinDirectory = rootDirectory->GetDirectory(JOIN_INDEX_PARTITION_DIR_NAME, true);
    }
    MakeIncSegment(joinDirectory, joinSegmentId, needSub, ts);
    return joinSegmentId;
}
}} // namespace indexlib::index_base
