#ifndef __INDEXLIB_COUNTERMAPTEST_H
#define __INDEXLIB_COUNTERMAPTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/counter/counter_map.h"

IE_NAMESPACE_BEGIN(util);

class CounterMapTest : public INDEXLIB_TESTBASE
{
public:
    CounterMapTest();
    ~CounterMapTest();

    DECLARE_CLASS_NAME(CounterMapTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetNotExistCounter();
    void TestGetExistCounter();
    void TestGetByNodePath();
    void TestMerge();
    void TestMergeException(); 
    void TestJsonize();
    void TestJsonizeException();
    void TestFindCounters();
    void TestCreateMultiCounter();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetNotExistCounter);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetExistCounter);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetByNodePath);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestMergeException);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestJsonizeException);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestFindCounters);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestCreateMultiCounter);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COUNTERMAPTEST_H
