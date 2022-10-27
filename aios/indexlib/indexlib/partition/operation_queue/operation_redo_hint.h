#ifndef __INDEXLIB_OPERATION_REDO_HINT_H
#define __INDEXLIB_OPERATION_REDO_HINT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(partition);

class OperationRedoHint
{
public:
    OperationRedoHint()
    {
        Reset();
    }
    ~OperationRedoHint() {}
public:
    bool IsValid() const
    {
        return mSegId != INVALID_SEGMENTID
            && mDocId != INVALID_DOCID;
    }
    
    void Reset()
    {
        mSegId = INVALID_SEGMENTID;
        mDocId = INVALID_DOCID;
    }
    void SetSegmentId(segmentid_t segId)
    {
        mSegId = segId;
    }
    segmentid_t GetSegmentId() const
    {
        return mSegId;
    }
    void SetLocalDocId(docid_t localDocId)
    {
        mDocId = localDocId;
    }
    docid_t GetLocalDocId() const
    {
        return mDocId;
    }
    
private:
    segmentid_t mSegId;
    docid_t mDocId;
};

DEFINE_SHARED_PTR(OperationRedoHint);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_REDO_HINT_H
