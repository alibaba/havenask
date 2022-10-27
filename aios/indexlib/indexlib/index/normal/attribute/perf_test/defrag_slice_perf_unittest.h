#ifndef __INDEXLIB_DEFRAGSLICEPERFTEST_H
#define __INDEXLIB_DEFRAGSLICEPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(index);

class DefragSlicePerfTest : public INDEXLIB_TESTBASE
{
public:
    DefragSlicePerfTest();
    ~DefragSlicePerfTest();

    DECLARE_CLASS_NAME(DefragSlicePerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DefragSlicePerfTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFRAGSLICEPERFTEST_H
