#ifndef __INDEXLIB_OPENEXECUTORCHAINTEST_H
#define __INDEXLIB_OPENEXECUTORCHAINTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"

IE_NAMESPACE_BEGIN(partition);

class OpenExecutorChainTest : public INDEXLIB_TESTBASE
{
public:
    OpenExecutorChainTest();
    ~OpenExecutorChainTest();

    DECLARE_CLASS_NAME(OpenExecutorChainTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OpenExecutorChainTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPENEXECUTORCHAINTEST_H
