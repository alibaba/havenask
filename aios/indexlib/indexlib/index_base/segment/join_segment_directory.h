#ifndef __INDEXLIB_JOIN_SEGMENT_DIRECTORY_H
#define __INDEXLIB_JOIN_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class JoinSegmentDirectory : public SegmentDirectory
{
    static const segmentid_t JOIN_SEGMENT_ID_MASK = (segmentid_t)0x1 << 29;
public:
    JoinSegmentDirectory();
    ~JoinSegmentDirectory();
public:
    SegmentDirectory* Clone() override
    { return new JoinSegmentDirectory(*this); }

    void CommitVersion() override
    { return DoCommitVersion(false); }

    void AddSegment(segmentid_t segId, timestamp_t ts) override;

    segmentid_t FormatSegmentId(segmentid_t segId) const override
    { return ConvertToJoinSegmentId(segId); }

    bool IsMatchSegmentId(segmentid_t segId) const override
    { return IsJoinSegmentId(segId); }

    static segmentid_t ConvertToJoinSegmentId(segmentid_t segId)
    { return segId | JOIN_SEGMENT_ID_MASK; }

    static bool IsJoinSegmentId(segmentid_t segId)
    { return (segId & JOIN_SEGMENT_ID_MASK) > 0; }

protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_JOIN_SEGMENT_DIRECTORY_H
