#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <indexlib/index/normal/inverted_index/accessor/term_meta.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);

class FakePostingIteratorTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakePostingIteratorTest);

TEST_F(FakePostingIteratorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    IndexReaderPtr readerPtr(new FakeTextIndexReader("abc^2:0^1;0^19;0^190;\n"));
    Term term("abc", "dummy");
    PostingIteratorPtr postIterator(readerPtr->Lookup(term, true));
    TermMeta* termMeta = postIterator->GetTermMeta();
    ASSERT_TRUE(termMeta);
    ASSERT_EQ(3, termMeta->GetDocFreq());
    ASSERT_EQ(0, termMeta->GetTotalTermFreq());
}

TEST_F(FakePostingIteratorTest, testSimpleProcessWithPositions) { 
    HA3_LOG(DEBUG, "Begin Test!");
    IndexReaderPtr readerPtr(new FakeTextIndexReader("ALIBABA:1_1-0[20_0];10-1[10_1,20_0,30_1];\n"));
    Term term("ALIBABA", "dummy");
    PostingIteratorPtr postIterator(readerPtr->Lookup(term, true));
    TermMeta* termMeta = postIterator->GetTermMeta();
    ASSERT_TRUE(termMeta);
    ASSERT_EQ(2, termMeta->GetDocFreq());
    ASSERT_EQ(4, termMeta->GetTotalTermFreq());
}

IE_NAMESPACE_END(index);

