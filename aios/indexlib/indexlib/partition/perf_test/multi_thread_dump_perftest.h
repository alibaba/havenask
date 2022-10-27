#ifndef __INDEXLIB_MULTITHREADDUMPPERFTEST_H
#define __INDEXLIB_MULTITHREADDUMPPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(partition);

class MultiThreadDumpPerfTest : public INDEXLIB_TESTBASE
{
public:
    MultiThreadDumpPerfTest();
    ~MultiThreadDumpPerfTest();

    DECLARE_CLASS_NAME(MultiThreadDumpPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSingleThreadDump();
    void TestMultiThreadDump();

private:
    void DoMultiThreadDump(uint32_t threadCount,
                           bool enablePackageFile);

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiThreadDumpPerfTest, TestSingleThreadDump);
INDEXLIB_UNIT_TEST_CASE(MultiThreadDumpPerfTest, TestMultiThreadDump);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MULTITHREADDUMPPERFTEST_H
