#ifndef __INDEXLIB_BUILDING_SUMMARY_READER_H
#define __INDEXLIB_BUILDING_SUMMARY_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class BuildingSummaryReader
{
public:
    BuildingSummaryReader();
    ~BuildingSummaryReader();
    
public:
    void AddSegmentReader(docid_t baseDocId,
                          const InMemSummarySegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader)
        {
            mBaseDocIds.push_back(baseDocId);
            mSegmentReaders.push_back(inMemSegReader);
        }
    }

    bool GetDocument(docid_t docId, document::SearchSummaryDocument *summaryDoc) const
    {
        for (size_t i = 0; i < mSegmentReaders.size(); i++)
        {
            docid_t curBaseDocId = mBaseDocIds[i];
            if (docId < curBaseDocId)
            {
                return false;
            }
            if (mSegmentReaders[i]->GetDocument(docId - curBaseDocId, summaryDoc))
            {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<docid_t> mBaseDocIds;
    std::vector<InMemSummarySegmentReaderPtr> mSegmentReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSummaryReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDING_SUMMARY_READER_H
