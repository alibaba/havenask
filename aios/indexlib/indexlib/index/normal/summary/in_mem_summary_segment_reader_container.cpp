#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemSummarySegmentReaderContainer);

InMemSummarySegmentReaderContainer::InMemSummarySegmentReaderContainer() 
{
}

InMemSummarySegmentReaderContainer::~InMemSummarySegmentReaderContainer() 
{
}

const InMemSummarySegmentReaderPtr& InMemSummarySegmentReaderContainer::GetInMemSummarySegmentReader(
        summarygroupid_t summaryGroupId) const
{
    assert(summaryGroupId >= 0 && 
           summaryGroupId < (summarygroupid_t)mInMemSummarySegmentReaderVec.size());
    return mInMemSummarySegmentReaderVec[summaryGroupId];
}

void InMemSummarySegmentReaderContainer::AddReader(const SummarySegmentReaderPtr& summarySegmentReader)
{
    InMemSummarySegmentReaderPtr inMemSummarySegmentReader =
        DYNAMIC_POINTER_CAST(InMemSummarySegmentReader, summarySegmentReader);
    mInMemSummarySegmentReaderVec.push_back(inMemSummarySegmentReader);
}

IE_NAMESPACE_END(index);

