#ifndef __INDEXLIB_POOLUTILTEST_H
#define __INDEXLIB_POOLUTILTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/pool_util.h"

IE_NAMESPACE_BEGIN(misc);

class PoolUtilTest : public INDEXLIB_TESTBASE
{
public:
    PoolUtilTest();
    ~PoolUtilTest();

    DECLARE_CLASS_NAME(PoolUtilTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PoolUtilTest, TestSimpleProcess);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_POOLUTILTEST_H
