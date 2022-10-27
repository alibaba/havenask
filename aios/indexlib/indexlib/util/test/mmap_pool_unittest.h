#ifndef __INDEXLIB_MMAPPOOLTEST_H
#define __INDEXLIB_MMAPPOOLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/mmap_pool.h"

IE_NAMESPACE_BEGIN(util);

class MmapPoolTest : public INDEXLIB_TESTBASE
{
public:
    MmapPoolTest();
    ~MmapPoolTest();

    DECLARE_CLASS_NAME(MmapPoolTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MmapPoolTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MMAPPOOLTEST_H
