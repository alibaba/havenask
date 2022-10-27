#ifndef __INDEXLIB_VERSION_MAKER_H
#define __INDEXLIB_VERSION_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index_base);

class VersionMaker
{
public:
    VersionMaker();
    ~VersionMaker();
public:
    static Version Make(versionid_t versionId, 
                        const std::string& incSegmentIds,
                        const std::string& rtSegmentIds = "",
                        const std::string& joinSegmentIds = "",
                        timestamp_t ts = INVALID_TIMESTAMP);

    static Version Make(file_system::DirectoryPtr rootDirectory,
                        versionid_t versionId, 
                        const std::string& incSegmentIds,
                        const std::string& rtSegmentIds = "",
                        const std::string& joinSegmentIds = "",
                        timestamp_t ts = INVALID_TIMESTAMP,
                        bool needSub = false);

    static segmentid_t MakeIncSegment(file_system::DirectoryPtr rootDirectory,
                                      segmentid_t rawSegId,
                                      bool needSub = false,
                                      int64_t ts = INVALID_TIMESTAMP);
    static segmentid_t MakeRealtimeSegment(file_system::DirectoryPtr rootDirectory,
            segmentid_t rawSegId,
            bool needSub = false,
            int64_t ts = INVALID_TIMESTAMP);
    static segmentid_t MakeJoinSegment(file_system::DirectoryPtr rootDirectory,
            segmentid_t rawSegId,
            bool needSub = false,
            int64_t ts = INVALID_TIMESTAMP);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionMaker);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_VERSION_MAKER_H
