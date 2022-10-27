#ifndef __INDEXLIB_SLICEFILETEST_H
#define __INDEXLIB_SLICEFILETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(file_system);

class SliceFileTest : public INDEXLIB_TESTBASE_WITH_PARAM<FSStorageType>
{
public:
    SliceFileTest();
    ~SliceFileTest();

    DECLARE_CLASS_NAME(SliceFileTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestExistInCacheBeforeClose();
    void TestMetricsIncrease();
    void TestMetricsDecrease();
    void TestCreateSliceFileWriter();
    void TestCreateSliceFileReader();
    void TestCacheHit();
    void TestRemoveSliceFile();
    void TestDisableCache();

private:
    void CheckSliceFileMetrics(const StorageMetrics& metrics, int64_t fileCount,
                               int64_t fileLength, int64_t totalFileLength, 
                               int lineNo);
private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(p1, SliceFileTest, testing::Values(FSST_LOCAL, FSST_IN_MEM));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestExistInCacheBeforeClose);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestMetricsIncrease);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestMetricsDecrease);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestCreateSliceFileWriter);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestCreateSliceFileReader);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(SliceFileTest, TestRemoveSliceFile);
INDEXLIB_UNIT_TEST_CASE(SliceFileTest, TestDisableCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICEFILETEST_H
