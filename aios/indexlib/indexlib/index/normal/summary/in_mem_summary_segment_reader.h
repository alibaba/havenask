#ifndef __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/summary/summary_define.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"

IE_NAMESPACE_BEGIN(index);

class InMemSummarySegmentReader : public SummarySegmentReader
{
public:
    InMemSummarySegmentReader(const config::SummaryGroupConfigPtr& summaryGroupConfig,
                              SummaryDataAccessor* accessor);
    ~InMemSummarySegmentReader();

public:
    bool GetDocument(docid_t localDocId,
                     document::SearchSummaryDocument *summaryDoc) const override;

    size_t GetRawDataLength(docid_t localDocId) override
    { assert(false); return 0; }

    void GetRawData(docid_t localDocId, char* rawData, size_t len) override
    { assert(false); }
    
    size_t GetDocCount() { return mAccessor->GetDocCount(); }
private:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    SummaryDataAccessor* mAccessor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemSummarySegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_SUMMARY_SEGMENT_READER_H
