#ifndef __INDEXLIB_POSTINGEXECUTORTEST_H
#define __INDEXLIB_POSTINGEXECUTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(merger, AndPostingExecutor);
IE_NAMESPACE_BEGIN(merger);

class PostingExecutorTest : public INDEXLIB_TESTBASE
{
public:
    PostingExecutorTest();
    ~PostingExecutorTest();

    DECLARE_CLASS_NAME(PostingExecutorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAndQueryOneTerm();
    void TestAndQueryTwoTermQuery1();
    void TestAndQueryTwoTermQuery2();
    void TestAndQueryTwoTermQuery3();
    void TestAndQueryThreeTermQuery();

private:
    AndPostingExecutorPtr PrepareAndPostingExecutor(
        const std::vector<std::vector<docid_t>> docLists);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingExecutorTest, TestAndQueryOneTerm);
INDEXLIB_UNIT_TEST_CASE(PostingExecutorTest, TestAndQueryTwoTermQuery1);
INDEXLIB_UNIT_TEST_CASE(PostingExecutorTest, TestAndQueryTwoTermQuery2);
INDEXLIB_UNIT_TEST_CASE(PostingExecutorTest, TestAndQueryTwoTermQuery3);
INDEXLIB_UNIT_TEST_CASE(PostingExecutorTest, TestAndQueryThreeTermQuery);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_POSTINGEXECUTORTEST_H
