#ifndef __INDEXLIB_DEVIRTUALTEST_H
#define __INDEXLIB_DEVIRTUALTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(misc);

class DeVirtualTest : public INDEXLIB_TESTBASE
{
public:
    DeVirtualTest();
    ~DeVirtualTest();

    DECLARE_CLASS_NAME(DeVirtualTest);
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeVirtualTest, TestSimpleProcess);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_DEVIRTUALTEST_H
