#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemSummarySegmentReader);

InMemSummarySegmentReader::InMemSummarySegmentReader(
        const SummaryGroupConfigPtr& summaryGroupConfig,
        SummaryDataAccessor* accessor)
    : mSummaryGroupConfig(summaryGroupConfig)
    , mAccessor(accessor)
{
}

InMemSummarySegmentReader::~InMemSummarySegmentReader() 
{
}

bool InMemSummarySegmentReader::GetDocument(
        docid_t localDocId, SearchSummaryDocument *summaryDoc) const
{
    if ((uint64_t)localDocId >= mAccessor->GetDocCount()) 
    {
        return false;
    }

    uint8_t* value = NULL;
    uint32_t size = 0;
    mAccessor->ReadData(localDocId, value, size);

    //TODO: ADD UT
    if (size == 0)
    {
        return false;
    }

    SummaryGroupFormatter formatter(mSummaryGroupConfig);
    if (formatter.DeserializeSummary(summaryDoc, (char*)value, (size_t)size))
    {
        return true;
    }
    INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                         "Deserialize in mem summary[docid = %d] FAILED.", localDocId);
    return false; 
}

IE_NAMESPACE_END(index);

