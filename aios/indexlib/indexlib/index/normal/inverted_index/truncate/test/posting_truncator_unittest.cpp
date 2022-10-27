#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/posting_truncator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class FakePostingTruncator : public PostingTruncator
{
public:
    FakePostingTruncator(uint64_t minDocCountToReserve, 
                         uint64_t maxDocCountToReserve,
                         const DocDistinctorPtr& docDistinctor)
        : PostingTruncator(minDocCountToReserve, maxDocCountToReserve, docDistinctor)
    {
    };

    ~FakePostingTruncator() {};

public:
    void AddDoc(docid_t docId) override
    {
        mDocIds.push_back(docId);
    }

    void Truncate() override
    {
    }
};

class PostingTruncatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PostingTruncatorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForReset()
    {
        FakePostingTruncator postingTruncator(10, 15, DocDistinctorPtr());
        postingTruncator.AddDoc((docid_t)5);

        postingTruncator.Reset();
        INDEXLIB_TEST_EQUAL((docid_t)INVALID_DOCID, postingTruncator.GetLastDocId());
        INDEXLIB_TEST_TRUE(postingTruncator.GetTruncatedDocIdVector().empty());
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PostingTruncatorTest);

INDEXLIB_UNIT_TEST_CASE(PostingTruncatorTest, TestCaseForReset);

IE_NAMESPACE_END(index);
