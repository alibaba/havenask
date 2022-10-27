#ifndef __INDEXLIB_OPERATION_CURSOR_H
#define __INDEXLIB_OPERATION_CURSOR_H

IE_NAMESPACE_BEGIN(partition);

struct OperationCursor
{
    segmentid_t segId;
    int32_t pos;
    OperationCursor(segmentid_t segmentId = INVALID_SEGMENTID,
                    int32_t position = -1)
        : segId(segmentId)
        , pos(position) {}
    
    bool operator < (const OperationCursor& other) const
    {
        if (segId < other.segId)
        {
            return true;
        }
        if (segId == other.segId)
        {
            return pos < other.pos;
        }
        return false;
    }

    bool operator == (const OperationCursor& other) const
    {
        return (segId == other.segId) && (pos == other.pos);
    }
    
    bool operator >= (const OperationCursor& other) const
    {
        return !(*this < other);
    }
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_CURSOR_H
