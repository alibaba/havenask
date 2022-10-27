#ifndef __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_CONTAINER_H
#define __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class InMemSummarySegmentReaderContainer : public SummarySegmentReader
{
public:
    InMemSummarySegmentReaderContainer();
    ~InMemSummarySegmentReaderContainer();

public:
    const InMemSummarySegmentReaderPtr& GetInMemSummarySegmentReader(
            summarygroupid_t summaryGroupId) const;

    void AddReader(const SummarySegmentReaderPtr& summarySegmentReader);

public:
    bool GetDocument(docid_t localDocId,
                     document::SearchSummaryDocument *summaryDoc) const override
    { assert(false); return false; }

    size_t GetRawDataLength(docid_t localDocId) override
    { assert(false); return 0; }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override
    { assert(false); }

private:
    typedef std::vector<InMemSummarySegmentReaderPtr> InMemSummarySegmentReaderVec;
    InMemSummarySegmentReaderVec mInMemSummarySegmentReaderVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSummarySegmentReaderContainer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_CONTAINER_H
