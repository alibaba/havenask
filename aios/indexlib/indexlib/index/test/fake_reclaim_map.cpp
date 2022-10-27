#include "indexlib/index/test/fake_reclaim_map.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_reader.h"

using namespace std;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, FakeReclaimMap);

void FakeReclaimMap::Init(const SegmentMergeInfos& segMergeInfos, 
                          const DeletionMapReaderPtr& deletionMapReader,
                          bool needReverse)
{
    uint32_t docCount = 0;
    vector<DocWeight> tmpDocs;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        docCount += segMergeInfos[i].segmentInfo.docCount;
    }
    mDocIdArray.resize(docCount);
    for (uint32_t i = 0; i < docCount; i++)
    {
        mDocIdArray[i] = INVALID_DOCID;
    }

    uint32_t segCount = segMergeInfos.size();
    for (size_t i = 0; i < segCount; ++i)
    {
        docid_t baseDocId = segMergeInfos[i].baseDocId;
        for (uint32_t j = 0; j < segMergeInfos[i].segmentInfo.docCount; j++)
        {
            if (!deletionMapReader->IsDeleted((docid_t)j + baseDocId))
            {
                float weight = (float)i / 10 + j;
                DocWeight docWeight;
                docWeight.weight = weight;
                docWeight.oldDocId = baseDocId + j;
                tmpDocs.push_back(docWeight);
            }
        }
    }
    sort(tmpDocs.begin(), tmpDocs.end(), DocWeightComparator());
            
    for (size_t i =0; i < tmpDocs.size(); i++)
    {
        docid_t oldDocId = tmpDocs[i].oldDocId;
        mDocIdArray[oldDocId] = i;
    }

    if (needReverse)
    {
        InitReverseDocIdArray(segMergeInfos);
    }
}


IE_NAMESPACE_END(index);

