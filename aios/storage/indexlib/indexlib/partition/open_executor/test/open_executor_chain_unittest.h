#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
