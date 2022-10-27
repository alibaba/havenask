#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyPostingIteratorTest 
    : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PrimaryKeyPostingIteratorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForUnpack()
    {
        docid_t docId = 3;
        PrimaryKeyPostingIterator it(3);
        INDEXLIB_TEST_EQUAL(docId, it.SeekDoc(2));

        TermMatchData termMatchData;
        it.Unpack(termMatchData);
        
        INDEXLIB_TEST_TRUE(termMatchData.IsMatched());
        INDEXLIB_TEST_TRUE(!termMatchData.HasDocPayload());
        INDEXLIB_TEST_EQUAL(1, termMatchData.GetTermFreq());
        INDEXLIB_TEST_TRUE(termMatchData.GetInDocPositionState() != NULL);

        INDEXLIB_TEST_EQUAL(INVALID_DOCID, it.SeekDoc(2));
    }

    void TestCaseForSeekFail()
    {
        PrimaryKeyPostingIterator it(3);
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, it.SeekDoc(4));
    }

    void TestCaseForClone()
    {
        PrimaryKeyPostingIterator it(3);
        docid_t docId = it.SeekDoc(2);
        INDEXLIB_TEST_EQUAL(3, docId);

        PostingIterator *iter = it.Clone();
        INDEXLIB_TEST_EQUAL(3, iter->SeekDoc(3));
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, iter->SeekDoc(3));
        delete iter;
    }

    void TestCaseForReset()
    {
        PrimaryKeyPostingIterator it(3);
        docid_t docId = it.SeekDoc(2);
        INDEXLIB_TEST_EQUAL(3, docId);

        it.Reset();
        INDEXLIB_TEST_EQUAL(3, it.SeekDoc(3));
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, it.SeekDoc(3));
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPostingIteratorTest, TestCaseForUnpack);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPostingIteratorTest, TestCaseForSeekFail);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPostingIteratorTest, TestCaseForClone);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyPostingIteratorTest, TestCaseForReset);

IE_NAMESPACE_END(index);
