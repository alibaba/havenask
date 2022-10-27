#include "indexlib/index/normal/inverted_index/accessor/term_posting_info.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TermPostingInfo);

TermPostingInfo::TermPostingInfo() 
    : mPostingSkipSize(0)
    , mPostingListSize(0)
    , mPositionSkipSize(0)
    , mPositionListSize(0)
{
}

TermPostingInfo::~TermPostingInfo() 
{
}

void TermPostingInfo::LoadPosting(ByteSliceReader* sliceReader)
{
    assert(sliceReader != NULL);
    mPostingSkipSize = sliceReader->ReadVUInt32();
    mPostingListSize = sliceReader->ReadVUInt32();
}

void TermPostingInfo::LoadPositon(ByteSliceReader* sliceReader)
{
    assert(sliceReader != NULL);
    mPositionSkipSize = sliceReader->ReadVUInt32();
    mPositionListSize = sliceReader->ReadVUInt32();
}

IE_NAMESPACE_END(index);

