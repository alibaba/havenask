#ifndef __INDEXLIB_DOC_PAYLOAD_FILTER_PROCESSOR_H
#define __INDEXLIB_DOC_PAYLOAD_FILTER_PROCESSOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_filter_processor.h"

IE_NAMESPACE_BEGIN(index);

class DocPayloadFilterProcessor : public DocFilterProcessor
{
public:
    DocPayloadFilterProcessor(const config::DiversityConstrain& constrain);
    ~DocPayloadFilterProcessor();

public:
    bool BeginFilter(dictkey_t key, const PostingIteratorPtr& postingIt);
    bool IsFiltered(docid_t docId);
    void SetTruncateMetaReader(const TruncateMetaReaderPtr &metaReader){};

    // for test
    void GetFilterRange(int64_t &min, int64_t &max);

private:
    int64_t mMinValue;
    int64_t mMaxValue;
    uint64_t mMask;
    PostingIteratorPtr mPostingIt;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocPayloadFilterProcessor);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_PAYLOAD_FILTER_PROCESSOR_H
