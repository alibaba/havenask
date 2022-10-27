#ifndef __INDEXLIB_SEGMENT_DATA_BASE_H
#define __INDEXLIB_SEGMENT_DATA_BASE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentDataBase
{
public:
    SegmentDataBase();
    SegmentDataBase(const SegmentDataBase& other);
    ~SegmentDataBase();

public:
    SegmentDataBase& operator=(const SegmentDataBase& segData);
    void SetBaseDocId(exdocid_t baseDocId) 
    { mBaseDocId = baseDocId; }
    exdocid_t GetBaseDocId() const { return mBaseDocId; }

    void SetSegmentId(segmentid_t segmentId) 
    { mSegmentId = segmentId;}
    segmentid_t GetSegmentId() const { return mSegmentId; }
    void SetSegmentDirName(const std::string& segDirName)
    { mSegmentDirName = segDirName; }
    const std::string& GetSegmentDirName() const { return mSegmentDirName; }

protected:
    exdocid_t mBaseDocId;
    segmentid_t mSegmentId;
    std::string mSegmentDirName;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDataBase);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_DATA_BASE_H
