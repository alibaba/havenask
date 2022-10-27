#ifndef __INDEXLIB_SUMMARY_SEGMENT_READER_H
#define __INDEXLIB_SUMMARY_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"

IE_NAMESPACE_BEGIN(index);

class SummarySegmentReader
{
public:
    SummarySegmentReader() {}
    virtual ~SummarySegmentReader() {}

public:
    /**
     * New get document summary by docid
     */
    virtual bool GetDocument(docid_t localDocId, document::SearchSummaryDocument *summaryDoc) const = 0;

    virtual size_t GetRawDataLength(docid_t localDocId) = 0;

    virtual void GetRawData(docid_t localDocId, char* rawData, size_t len) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummarySegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_SEGMENT_READER_H
