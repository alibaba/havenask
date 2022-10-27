#ifndef __INDEXLIB_SIMPLEPOOLTEST_H
#define __INDEXLIB_SIMPLEPOOLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(util);

class SimplePoolTest : public INDEXLIB_TESTBASE
{
public:
    SimplePoolTest();
    ~SimplePoolTest();

    DECLARE_CLASS_NAME(SimplePoolTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestAbnormalCase();
    void TestPoolUtil();
    void TestSimplePoolAllocator();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SimplePoolTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SimplePoolTest, TestAbnormalCase);
INDEXLIB_UNIT_TEST_CASE(SimplePoolTest, TestPoolUtil);
INDEXLIB_UNIT_TEST_CASE(SimplePoolTest, TestSimplePoolAllocator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SIMPLEPOOLTEST_H
