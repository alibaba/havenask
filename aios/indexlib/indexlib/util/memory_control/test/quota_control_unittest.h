#ifndef __INDEXLIB_QUOTACONTROLTEST_H
#define __INDEXLIB_QUOTACONTROLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/quota_control.h"

IE_NAMESPACE_BEGIN(util);

class QuotaControlTest : public INDEXLIB_TESTBASE
{
public:
    QuotaControlTest();
    ~QuotaControlTest();

    DECLARE_CLASS_NAME(QuotaControlTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(QuotaControlTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_QUOTACONTROLTEST_H
