#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_decoder.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemBitmapIndexDecoder);

InMemBitmapIndexDecoder::InMemBitmapIndexDecoder()
    : mBitmapItemCount(0)
    , mBitmapData(NULL)
{
}

InMemBitmapIndexDecoder::~InMemBitmapIndexDecoder() 
{
}

void InMemBitmapIndexDecoder::Init(const BitmapPostingWriter* postingWriter)
{
    assert(postingWriter);
    mTermMeta.SetPayload(postingWriter->GetTermPayload());
    mTermMeta.SetDocFreq(postingWriter->GetDF());
    mTermMeta.SetTotalTermFreq(postingWriter->GetTotalTF());

    MEMORY_BARRIER();

    const ExpandableBitmap* bitmap = postingWriter->GetBitmapData();
    mBitmapItemCount = postingWriter->GetLastDocId() + 1;
    MEMORY_BARRIER();
    mBitmapData = bitmap->GetData();
}

IE_NAMESPACE_END(index);

