#include <autil/StringUtil.h>
#include "indexlib/file_system/test/storage_metrics_unittest.h"
#include "indexlib/file_system/storage_metrics.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, StorageMetricsTest);

StorageMetricsTest::StorageMetricsTest()
{
}

StorageMetricsTest::~StorageMetricsTest()
{
}

void StorageMetricsTest::CaseSetUp()
{
}

void StorageMetricsTest::CaseTearDown()
{
}

void StorageMetricsTest::CheckFileMetrics(StorageMetrics& metrics, FSFileType type)
{
    metrics.IncreaseFile(type, 0);
    ASSERT_EQ(0, metrics.GetFileLength(type));
    ASSERT_EQ(1, metrics.GetFileCount(type));

    metrics.DecreaseFile(type, 0);
    ASSERT_EQ(0, metrics.GetFileLength(type));
    ASSERT_EQ(0, metrics.GetFileCount(type));

    metrics.IncreaseFile(type, 100);
    metrics.IncreaseFile(type, 200);
    metrics.DecreaseFile(type, 100);
    metrics.IncreaseFile(type, 300);
    metrics.DecreaseFile(type, 300);
    metrics.IncreaseFile(type, 300);

    ASSERT_EQ(500, metrics.GetFileLength(type));
    ASSERT_EQ(2, metrics.GetFileCount(type));
}

void StorageMetricsTest::TestForFileMetrics()
{
    StorageMetrics metrics;
    CheckFileMetrics(metrics, FSFT_IN_MEM);
    CheckFileMetrics(metrics, FSFT_MMAP);
    CheckFileMetrics(metrics, FSFT_BUFFERED);
    CheckFileMetrics(metrics, FSFT_BLOCK);
    CheckFileMetrics(metrics, FSFT_SLICE);

    ASSERT_EQ(10, metrics.GetTotalFileCount());
    ASSERT_EQ(2500, metrics.GetTotalFileLength());
}

void StorageMetricsTest::CheckFileCacheMetrics(const StorageMetrics& metrics,
        int64_t hitCount, int64_t missCount, int64_t replaceCount, 
        int64_t removeCount, int lineNo)
{
    SCOPED_TRACE(string("see line:") + StringUtil::toString(lineNo) + "]");
    
    ASSERT_EQ(hitCount, metrics.GetFileCacheMetrics().hitCount.getValue());
    ASSERT_EQ(missCount, metrics.GetFileCacheMetrics().missCount.getValue());
    ASSERT_EQ(replaceCount, metrics.GetFileCacheMetrics().replaceCount.getValue());
    ASSERT_EQ(removeCount, metrics.GetFileCacheMetrics().removeCount.getValue());
}

void StorageMetricsTest::TestForFileCacheMetrics()
{
    StorageMetrics metrics;
    metrics.IncreaseFileCacheHit();
    CheckFileCacheMetrics(metrics, 1, 0, 0, 0, __LINE__);
    metrics.IncreaseFileCacheMiss();
    CheckFileCacheMetrics(metrics, 1, 1, 0, 0, __LINE__);
    metrics.IncreaseFileCacheReplace();
    CheckFileCacheMetrics(metrics, 1, 1, 1, 0, __LINE__);
    metrics.IncreaseFileCacheRemove();
    CheckFileCacheMetrics(metrics, 1, 1, 1, 1, __LINE__);
}

IE_NAMESPACE_END(file_system);

