#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"

#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::index;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlibv2 { namespace index {

class InMemBitmapIndexSegmentReaderTest : public TESTBASE
{
public:
    using BitmapPostingTable = InMemBitmapIndexSegmentReader::BitmapPostingTable;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(InMemBitmapIndexSegmentReaderTest, TestGetSegmentPosting)
{
    {
        BitmapPostingTable postingTable(HASHMAP_INIT_SIZE);
        BitmapPostingWriter postingWriter;
        BitmapPostingWriter* writer = nullptr;
        postingTable.Insert(InvertedIndexUtil::GetRetrievalHashKey(true, 1234), &postingWriter);

        InMemBitmapIndexSegmentReader inMemSegReader(&postingTable, true, &writer);
        PostingFormatOption option;
        SegmentPosting segPosting(option);

        ASSERT_FALSE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo(4321), 100, segPosting, nullptr, {},
                                                      nullptr));
        ASSERT_TRUE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo(1234), 100, segPosting, nullptr, {},
                                                     nullptr));
        ASSERT_EQ(&postingWriter, segPosting.GetInMemPostingWriter());
        ASSERT_EQ((docid_t)100, segPosting.GetBaseDocId());

        ASSERT_FALSE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo::NULL_TERM, 100, segPosting, nullptr,
                                                      {}, nullptr));
        writer = &postingWriter;
        ASSERT_TRUE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo::NULL_TERM, 100, segPosting, nullptr,
                                                     {}, nullptr));
        ASSERT_EQ(&postingWriter, segPosting.GetInMemPostingWriter());
        ASSERT_EQ((docid_t)100, segPosting.GetBaseDocId());
    }

    {
        BitmapPostingTable postingTable(HASHMAP_INIT_SIZE);
        BitmapPostingWriter postingWriter;
        postingTable.Insert(InvertedIndexUtil::GetRetrievalHashKey(false, 1234), &postingWriter);

        InMemBitmapIndexSegmentReader inMemSegReader(&postingTable, false);
        PostingFormatOption option;
        SegmentPosting segPosting(option);

        ASSERT_FALSE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo(4321), 100, segPosting, nullptr, {},
                                                      nullptr));
        ASSERT_TRUE(inMemSegReader.GetSegmentPosting(indexlib::index::DictKeyInfo(1234), 100, segPosting, nullptr, {},
                                                     nullptr));
        ASSERT_EQ(&postingWriter, segPosting.GetInMemPostingWriter());
        ASSERT_EQ((docid_t)100, segPosting.GetBaseDocId());
    }
}

}} // namespace indexlibv2::index
