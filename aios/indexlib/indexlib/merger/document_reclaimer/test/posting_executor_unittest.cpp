#include "indexlib/merger/document_reclaimer/and_posting_executor.h"
#include "indexlib/merger/document_reclaimer/term_posting_executor.h"
#include "indexlib/merger/document_reclaimer/test/posting_executor_unittest.h"
#include "indexlib/testlib/fake_posting_iterator.h"

using namespace std;
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PostingExecutorTest);

PostingExecutorTest::PostingExecutorTest()
{
}

PostingExecutorTest::~PostingExecutorTest()
{
}

void PostingExecutorTest::CaseSetUp()
{
}

void PostingExecutorTest::CaseTearDown()
{
}

AndPostingExecutorPtr PostingExecutorTest::PrepareAndPostingExecutor(
    const vector<vector<docid_t>> docLists)
{
    vector<PostingExecutorPtr> postingExecutors;
    postingExecutors.reserve(docLists.size());
    for (auto docVec : docLists)
    {
        FakePostingIterator::Postings postings;
        FakePostingIterator::Posting posting;
        FakePostingIterator::RichPostings richPostings;        
        postings.reserve(docVec.size());
        for (auto docid: docVec)
        {
            posting.docid = docid;
            postings.push_back(posting);
        }
        richPostings.first = 1;
        richPostings.second = postings;
        PostingIteratorPtr iterator(new FakePostingIterator(richPostings, 10000000));
        PostingExecutorPtr executor(new TermPostingExecutor(iterator));
        postingExecutors.push_back(executor);
    }
    AndPostingExecutorPtr andExecutor(new AndPostingExecutor(postingExecutors));
    return andExecutor;
}

void PostingExecutorTest::TestAndQueryOneTerm()
{
    vector<vector<docid_t>> postings = {{0,1,3,5,7,9}};
    AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);
    ASSERT_EQ(0, executor->Seek(0));
    ASSERT_EQ(1, executor->Seek(1));
    ASSERT_EQ(3, executor->Seek(2));
    ASSERT_EQ(3, executor->Seek(3)); 
    ASSERT_EQ(5, executor->Seek(4));
    ASSERT_EQ(5, executor->Seek(5));
    ASSERT_EQ(7, executor->Seek(6));
    ASSERT_EQ(7, executor->Seek(7));
    ASSERT_EQ(9, executor->Seek(8));
    ASSERT_EQ(9, executor->Seek(9));
    ASSERT_EQ(END_DOCID, executor->Seek(10));
    ASSERT_EQ(END_DOCID, executor->Seek(END_DOCID)); 
}


void PostingExecutorTest::TestAndQueryTwoTermQuery1()
{
    vector<vector<docid_t>> postings = {{1,3,5}, {2, 4}};
    AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);
    ASSERT_EQ(END_DOCID, executor->Seek(0)); 
}

void PostingExecutorTest::TestAndQueryTwoTermQuery2()
{
    {
        vector<vector<docid_t>> postings = {{1,2,3}, {2, 3, 4}};
        AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);
        ASSERT_EQ((docid_t)2, executor->Seek(0));
        // test repeatly seek the same docid
        ASSERT_EQ((docid_t)2, executor->Seek(2));
        ASSERT_EQ((docid_t)2, executor->Seek(2));
        ASSERT_EQ((docid_t)3, executor->Seek(3));
        // test seek back
        ASSERT_EQ((docid_t)3, executor->Seek(2));        
        ASSERT_EQ(END_DOCID, executor->Seek(4));
        ASSERT_EQ(END_DOCID, executor->Seek(END_DOCID));
        ASSERT_EQ(END_DOCID, executor->Seek(END_DOCID));        
    }
    {
        vector<vector<docid_t>> postings = {{1,2,3,100}, {2,3,4,5,6,7,8,9}};
        AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);
        ASSERT_EQ((docid_t)2, executor->Seek(0));
        ASSERT_EQ((docid_t)2, executor->Seek(2));
        ASSERT_EQ((docid_t)3, executor->Seek(3));
        ASSERT_EQ(END_DOCID, executor->Seek(4));
        ASSERT_EQ(END_DOCID, executor->Seek(END_DOCID));
    }
}

void PostingExecutorTest::TestAndQueryTwoTermQuery3()
{
    vector<vector<docid_t>> postings = {{1, 8}, {1, 8}};
    AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);

    ASSERT_EQ((docid_t)8, executor->Seek(7));
    ASSERT_EQ(END_DOCID, executor->Seek(10));
}

void PostingExecutorTest::TestAndQueryThreeTermQuery()
{
    vector<vector<docid_t>> postings = {{1,3,6,7,8},{2,3,5,8},{2,3,6,8}};
    AndPostingExecutorPtr executor = PrepareAndPostingExecutor(postings);
    ASSERT_EQ((docid_t)3, executor->Seek(0));
    ASSERT_EQ((docid_t)8, executor->Seek(4));
    ASSERT_EQ(END_DOCID, executor->Seek(10));
}

IE_NAMESPACE_END(merger);

