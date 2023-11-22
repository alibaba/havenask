#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class VersionMaker
{
public:
    VersionMaker();
    ~VersionMaker();

public:
    static Version Make(versionid_t versionId, const std::string& incSegmentIds, const std::string& rtSegmentIds = "",
                        const std::string& joinSegmentIds = "", timestamp_t ts = INVALID_TIMESTAMP);

    static Version Make(file_system::DirectoryPtr rootDirectory, versionid_t versionId,
                        const std::string& incSegmentIds, const std::string& rtSegmentIds = "",
                        const std::string& joinSegmentIds = "", timestamp_t ts = INVALID_TIMESTAMP,
                        bool needSub = false);

    static segmentid_t MakeIncSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId,
                                      bool needSub = false, int64_t ts = INVALID_TIMESTAMP);
    static segmentid_t MakeRealtimeSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId,
                                           bool needSub = false, int64_t ts = INVALID_TIMESTAMP);
    static segmentid_t MakeJoinSegment(file_system::DirectoryPtr rootDirectory, segmentid_t rawSegId,
                                       bool needSub = false, int64_t ts = INVALID_TIMESTAMP);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionMaker);
}} // namespace indexlib::index_base
