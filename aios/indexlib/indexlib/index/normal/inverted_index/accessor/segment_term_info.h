#ifndef __INDEXLIB_SEGMENT_TERM_INFO_H
#define __INDEXLIB_SEGMENT_TERM_INFO_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_iterator.h"

IE_NAMESPACE_BEGIN(index);

struct SegmentTermInfo
{
public:
    enum TermIndexMode { TM_NORMAL = 0, TM_BITMAP = 1 };

public:
    SegmentTermInfo()
        : segId(INVALID_SEGMENTID)
        , baseDocId(INVALID_DOCID)
        , key(0)
        , postingDecoder(NULL)
        , termIndexMode(TM_NORMAL)
    {
    }
    
    SegmentTermInfo(segmentid_t id, 
                    docid_t baseId,
                    const IndexIteratorPtr& it,
                    TermIndexMode mode = TM_NORMAL)
        : segId(id)
        , baseDocId(baseId)
        , indexIter(it)
        , key(0)
        , termIndexMode(mode)
    {
    }

    bool Next()
    {
        if (indexIter->HasNext())
        {
            postingDecoder = indexIter->Next(key);
            return true;
        }
        return false;
    }

    segmentid_t segId;
    docid_t baseDocId;

    IndexIteratorPtr indexIter;
    dictkey_t key;
    PostingDecoder* postingDecoder;
    TermIndexMode termIndexMode;
};

class SegmentTermInfoComparator
{
public:
    bool operator()(const SegmentTermInfo* item1, const SegmentTermInfo* item2)
    {
        if (item1->key != item2->key)
        {
            return item1->key > item2->key;
        }

        if (item1->termIndexMode != item2->termIndexMode)
        {
            return item1->termIndexMode > item2->termIndexMode;
        }
        return item1->segId > item2->segId;
    }
};

typedef std::vector<SegmentTermInfo*> SegmentTermInfos;
DEFINE_SHARED_PTR(SegmentTermInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_TERM_INFO_H
