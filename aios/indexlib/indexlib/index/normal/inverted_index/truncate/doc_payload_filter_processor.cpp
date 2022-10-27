#include "indexlib/index/normal/inverted_index/truncate/doc_payload_filter_processor.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocPayloadFilterProcessor);

DocPayloadFilterProcessor::DocPayloadFilterProcessor(
        const config::DiversityConstrain& constrain) 
{
    mMaxValue = constrain.GetFilterMaxValue();
    mMinValue = constrain.GetFilterMinValue();
    mMask = constrain.GetFilterMask();
}

DocPayloadFilterProcessor::~DocPayloadFilterProcessor() 
{
}

void DocPayloadFilterProcessor::GetFilterRange(int64_t &min, int64_t &max)
{
    min = mMinValue;
    max = mMaxValue;
}

bool DocPayloadFilterProcessor::BeginFilter(dictkey_t key, const PostingIteratorPtr& postingIt)
{
    mPostingIt = postingIt;
    return true;
}

bool DocPayloadFilterProcessor::IsFiltered(docid_t docId)
{
    assert(mPostingIt);
    docpayload_t value = mPostingIt->GetDocPayload();
    value &= (docpayload_t)mMask;
    return !(mMinValue <= (int64_t)value && (int64_t)value <= mMaxValue);
}

IE_NAMESPACE_END(index);

