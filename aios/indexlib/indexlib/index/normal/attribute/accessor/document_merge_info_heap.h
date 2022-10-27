#ifndef __INDEXLIB_DOCUMENT_MERGE_INFO_HEAP_H
#define __INDEXLIB_DOCUMENT_MERGE_INFO_HEAP_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"

IE_NAMESPACE_BEGIN(index);

class DocumentMergeInfoHeap
{
public:
    DocumentMergeInfoHeap();
    ~DocumentMergeInfoHeap();
    
public:
    void Init(const index_base::SegmentMergeInfos& segMergeInfos,
              const ReclaimMapPtr& reclaimMap);
    bool GetNext(DocumentMergeInfo& docMergeInfo);
    bool IsEmpty() const {
        return mCurrentDocInfo.newDocId == INVALID_DOCID;
    }

    const DocumentMergeInfo &Top() const {
        return mCurrentDocInfo;
    }

private:
    void LoadNextDoc();

private:
    index_base::SegmentMergeInfos mSegMergeInfos;
    ReclaimMapPtr mReclaimMap;
    std::vector<docid_t> mDocIdCursors;

    size_t mSegCursor;
    DocumentMergeInfo mCurrentDocInfo;
    std::vector<docid_t> mNextNewLocalDocIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentMergeInfoHeap);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCUMENT_MERGE_INFO_HEAP_H
