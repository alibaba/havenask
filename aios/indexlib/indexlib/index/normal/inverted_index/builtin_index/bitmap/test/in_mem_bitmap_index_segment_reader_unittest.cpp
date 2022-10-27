#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/in_mem_bitmap_index_segment_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

void InMemBitmapIndexSegmentReaderTest::CaseSetUp()
{
}

void InMemBitmapIndexSegmentReaderTest::CaseTearDown()
{
}

void InMemBitmapIndexSegmentReaderTest::TestGetSegmentPosting()
{
    {
        PostingTable postingTable(HASHMAP_INIT_SIZE);
        BitmapPostingWriter postingWriter;
        postingTable.Insert(IndexWriter::GetRetrievalHashKey(
                        true, 1234), &postingWriter);

        InMemBitmapIndexSegmentReader inMemSegReader(&postingTable, true);
        index::PostingFormatOption option;
        SegmentPosting segPosting(option);
    
        ASSERT_FALSE(inMemSegReader.GetSegmentPosting(4321, 100, segPosting, NULL));
        ASSERT_TRUE(inMemSegReader.GetSegmentPosting(1234, 100, segPosting, NULL));
        ASSERT_EQ(&postingWriter, segPosting.GetInMemPostingWriter());
        ASSERT_EQ((docid_t)100, segPosting.GetBaseDocId());
    }

    {
        PostingTable postingTable(HASHMAP_INIT_SIZE);
        BitmapPostingWriter postingWriter;
        postingTable.Insert(IndexWriter::GetRetrievalHashKey(
                        false, 1234), &postingWriter);

        InMemBitmapIndexSegmentReader inMemSegReader(&postingTable, false);
        index::PostingFormatOption option;
        SegmentPosting segPosting(option);
    
        ASSERT_FALSE(inMemSegReader.GetSegmentPosting(4321, 100, segPosting, NULL));
        ASSERT_TRUE(inMemSegReader.GetSegmentPosting(1234, 100, segPosting, NULL));
        ASSERT_EQ(&postingWriter, segPosting.GetInMemPostingWriter());
        ASSERT_EQ((docid_t)100, segPosting.GetBaseDocId());
    }
}

IE_NAMESPACE_END(index);
