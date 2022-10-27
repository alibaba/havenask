#include <ha3_sdk/testlib/index/FakeNumberIndexReader.h>
#include <ha3_sdk/testlib/index/FakeNumberPostingIterator.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

IE_NAMESPACE_USE(index);
IE_NAMESPACE_BEGIN(index);

class FakeNumberPostingIteratorTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakeNumberPostingIteratorTest);

TEST_F(FakeNumberPostingIteratorTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeNumberIndexReader fkNumIdxReader;
    fkNumIdxReader.setNumberValuesFromString("1: 3, 5, 7");
    FakeNumberPostingIterator *fkNumPostingIt =
        new FakeNumberPostingIterator(fkNumIdxReader.getNumberValues(), 1, 1);

    PostingIteratorPtr postingItPtr(fkNumPostingIt);

    ASSERT_EQ((docid_t) 3, postingItPtr->SeekDoc(0));
    ASSERT_EQ((docid_t) 5, postingItPtr->SeekDoc(4));
    ASSERT_EQ((docid_t) 7, postingItPtr->SeekDoc(6));
}

TEST_F(FakeNumberPostingIteratorTest, testRangeIterator) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeNumberIndexReader fkNumIdxReader;
    fkNumIdxReader.setNumberValuesFromString("1: 3, 7; 3: 5");
    FakeNumberPostingIterator *fkNumPostingIt =
        new FakeNumberPostingIterator(fkNumIdxReader.getNumberValues(), 1, 3);
    PostingIteratorPtr postingItPtr(fkNumPostingIt);

    ASSERT_EQ((docid_t) 3, postingItPtr->SeekDoc(0));
    ASSERT_EQ((docid_t) 5, postingItPtr->SeekDoc(4));
    ASSERT_EQ((docid_t) 7, postingItPtr->SeekDoc(6));
}

IE_NAMESPACE_END(index);
