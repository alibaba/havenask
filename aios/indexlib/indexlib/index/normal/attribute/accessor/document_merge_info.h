#ifndef __INDEXLIB_DOCUMENT_MERGE_INFO_H
#define __INDEXLIB_DOCUMENT_MERGE_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

struct DocumentMergeInfo
{
    DocumentMergeInfo()
        : segmentIndex(-1)
        , oldDocId(INVALID_DOCID)
        , newDocId(INVALID_DOCID)
        , targetSegmentIndex(0)
    {
    }

    DocumentMergeInfo(int32_t segIdx, docid_t oldId, docid_t newId,
                      uint32_t targetSegIdx)
        : segmentIndex(segIdx), oldDocId(oldId), newDocId(newId),
        targetSegmentIndex(targetSegIdx){}

    int32_t segmentIndex;
    docid_t oldDocId;
    docid_t newDocId; // docid in plan, not in target segment local; TODO: use target segment localId
    uint32_t targetSegmentIndex;
};

DEFINE_SHARED_PTR(DocumentMergeInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCUMENT_MERGE_INFO_H
