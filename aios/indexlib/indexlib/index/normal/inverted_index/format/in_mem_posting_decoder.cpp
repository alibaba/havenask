#include "indexlib/index/normal/inverted_index/format/in_mem_posting_decoder.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPostingDecoder);

InMemPostingDecoder::InMemPostingDecoder() 
    : mDocListDecoder(NULL)
    , mPositionListDecoder(NULL)
{
}

InMemPostingDecoder::~InMemPostingDecoder() 
{
}

IE_NAMESPACE_END(index);

