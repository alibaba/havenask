#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocumentMergeInfoHeap);

DocumentMergeInfoHeap::DocumentMergeInfoHeap()
    : mSegCursor(0)
{
}

DocumentMergeInfoHeap::~DocumentMergeInfoHeap()
{
}

void DocumentMergeInfoHeap::Init(const SegmentMergeInfos& segMergeInfos,
                                 const ReclaimMapPtr& reclaimMap)
{
    mSegMergeInfos = segMergeInfos;
    mReclaimMap = reclaimMap;

    uint32_t segCount = mSegMergeInfos.size();
    mDocIdCursors.resize(segCount, 0);


    auto targetSegCount = reclaimMap->GetTargetSegmentCount();
    mNextNewLocalDocIds.resize(targetSegCount, 0);

    LoadNextDoc();
}

void DocumentMergeInfoHeap::LoadNextDoc()
{
    uint32_t segCount = mSegMergeInfos.size();
    size_t i = 0;
    for (; i < segCount; ++i) {
        while (mDocIdCursors[mSegCursor] <
               (docid_t)mSegMergeInfos[mSegCursor].segmentInfo.docCount) {
            docid_t newId = mReclaimMap->GetNewId(
                mDocIdCursors[mSegCursor] + mSegMergeInfos[mSegCursor].baseDocId);
            if (newId != INVALID_DOCID) {
                auto newLocalInfo = mReclaimMap->GetLocalId(newId);
                auto targetSegIdx = newLocalInfo.first;
                auto newLocalId = newLocalInfo.second;
                if (newLocalId == mNextNewLocalDocIds[targetSegIdx]) {
                    mCurrentDocInfo.segmentIndex = mSegCursor;
                    mCurrentDocInfo.oldDocId = mDocIdCursors[mSegCursor];
                    mCurrentDocInfo.newDocId = newId;
                    mCurrentDocInfo.targetSegmentIndex = targetSegIdx;
                    return;
                }
                break;
            } else {
                mDocIdCursors[mSegCursor]++;
            }
        }
        ++mSegCursor;
        if (mSegCursor >= segCount) {
            mSegCursor = 0;
        }      
    }
    if (i >= segCount) {
        mCurrentDocInfo = DocumentMergeInfo();
    }
}

bool DocumentMergeInfoHeap::GetNext(DocumentMergeInfo& docMergeInfo)
{
    if (IsEmpty()) {
        return false;
    }

    docMergeInfo = mCurrentDocInfo;
    ++mNextNewLocalDocIds[docMergeInfo.targetSegmentIndex];
    mDocIdCursors[mSegCursor]++;
    LoadNextDoc();

    return true;
}

IE_NAMESPACE_END(index);

