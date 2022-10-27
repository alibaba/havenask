#ifndef __INDEXLIB_MUNMAPPERFTEST_H
#define __INDEXLIB_MUNMAPPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(file_system);

class MunmapPerfTest : public test::MultiThreadTestBase
{
public:
    MunmapPerfTest();
    ~MunmapPerfTest();

    DECLARE_CLASS_NAME(MunmapPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void DoWrite() override;
    void DoRead(int* status) override;

private:
    static void MakeData(const std::string& filePath, size_t size);

private:
    std::string mRootDir;
    std::string mFilePath;
    volatile bool mIsClosing;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MunmapPerfTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MUNMAPPERFTEST_H
