#ifndef __INDEXLIB_MMAPVECTORTEST_H
#define __INDEXLIB_MMAPVECTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/mmap_vector.h"

IE_NAMESPACE_BEGIN(util);

class MMapVectorTest : public INDEXLIB_TESTBASE
{
public:
    MMapVectorTest();
    ~MMapVectorTest();

    DECLARE_CLASS_NAME(MMapVectorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestPushBack();
    void TestEqual();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MMapVectorTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MMapVectorTest, TestPushBack);
INDEXLIB_UNIT_TEST_CASE(MMapVectorTest, TestEqual);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MMAPVECTORTEST_H
