#ifndef __INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H
#define __INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class RealtimeSegmentDirectory : public SegmentDirectory
{
    static const segmentid_t RT_SEGMENT_ID_MASK = (segmentid_t)0x1 << 30;
public:
    RealtimeSegmentDirectory(bool enableRecover = false);
    ~RealtimeSegmentDirectory();
public:
    SegmentDirectory* Clone() override
    { return new RealtimeSegmentDirectory(*this); }

    void CommitVersion() override
    { return DoCommitVersion(false); }

    segmentid_t FormatSegmentId(segmentid_t segId) const override
    { return ConvertToRtSegmentId(segId); }

    bool IsMatchSegmentId(segmentid_t segId) const override
    { return IsRtSegmentId(segId); }

    static segmentid_t ConvertToRtSegmentId(segmentid_t segId)
    { return segId | RT_SEGMENT_ID_MASK; }

    static bool IsRtSegmentId(segmentid_t segId)
    { return (segId & RT_SEGMENT_ID_MASK) > 0; }

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

private:
    void RemoveVersionFiles(const file_system::DirectoryPtr& directory);

private:
    bool mEnableRecover;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimeSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_REALTIME_SEGMENT_DIRECTORY_H
