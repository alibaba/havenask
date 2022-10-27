#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemBitmapIndexSegmentReader);

InMemBitmapIndexSegmentReader::InMemBitmapIndexSegmentReader(
        const PostingTable* postingTable, bool isNumberIndex)
    : mPostingTable(postingTable)
    , mIsNumberIndex(isNumberIndex)
{
}

InMemBitmapIndexSegmentReader::~InMemBitmapIndexSegmentReader() 
{
}

bool InMemBitmapIndexSegmentReader::GetSegmentPosting(
        dictkey_t key, docid_t baseDocId,
        SegmentPosting &segPosting, Pool* sessionPool) const
{
    assert(mPostingTable);
    PostingWriter* postingWriter = mPostingTable->Find(
            IndexWriter::GetRetrievalHashKey(mIsNumberIndex, key), NULL);
    if (postingWriter == NULL)
    {
        return false;
    }

    segPosting.Init(baseDocId, 0, postingWriter);
    return true; 
}

IE_NAMESPACE_END(index);

