#ifndef __INDEXLIB_STORAGEMETRICSTEST_H
#define __INDEXLIB_STORAGEMETRICSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/storage_metrics.h"

IE_NAMESPACE_BEGIN(file_system);

class StorageMetricsTest : public INDEXLIB_TESTBASE
{
public:
    StorageMetricsTest();
    ~StorageMetricsTest();

    DECLARE_CLASS_NAME(StorageMetricsTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestForFileMetrics();
    void TestForFileCacheMetrics();

private:
    void CheckFileMetrics(StorageMetrics& metrics, FSFileType type);
    void CheckFileCacheMetrics(const StorageMetrics& metrics,
                               int64_t hitCount, int64_t missCount,
                               int64_t replaceCount, int64_t removeCount,
                               int lineNo);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(StorageMetricsTest, TestForFileMetrics);
INDEXLIB_UNIT_TEST_CASE(StorageMetricsTest, TestForFileCacheMetrics);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_STORAGEMETRICSTEST_H
