#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemNormalIndexSegmentReader);

InMemNormalIndexSegmentReader::InMemNormalIndexSegmentReader(
        const PostingTable *postingTable, 
        const AttributeSegmentReaderPtr& sectionSegmentReader,
        const InMemBitmapIndexSegmentReaderPtr& bitmapSegmentReader,
        const IndexFormatOption& indexFormatOption)
    : mPostingTable(postingTable)
    , mSectionSegmentReader(sectionSegmentReader)
    , mBitmapSegmentReader(bitmapSegmentReader)
    , mIndexFormatOption(indexFormatOption)
{
}

InMemNormalIndexSegmentReader::~InMemNormalIndexSegmentReader() 
{
}

bool InMemNormalIndexSegmentReader::GetSegmentPosting(
        dictkey_t key, docid_t baseDocId,
        SegmentPosting &segPosting, Pool* sessionPool) const
{
    assert(mPostingTable);
    SegmentPosting inMemSegPosting(mIndexFormatOption.GetPostingFormatOption());
    PostingWriter* postingWriter = mPostingTable->Find(
            IndexWriter::GetRetrievalHashKey(
                    mIndexFormatOption.IsNumberIndex(), key), NULL);
    if (postingWriter == NULL)
    {
        return false;
    }

    inMemSegPosting.Init(baseDocId, 0, postingWriter);
    segPosting = inMemSegPosting;
    return true; 
}

IE_NAMESPACE_END(index);

