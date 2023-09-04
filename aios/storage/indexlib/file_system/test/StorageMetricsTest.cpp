#include "indexlib/file_system/StorageMetrics.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <stdint.h>
#include <string>

#include "autil/AtomicCounter.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {

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
    void CheckFileCacheMetrics(const StorageMetrics& metrics, int64_t hitCount, int64_t missCount, int64_t replaceCount,
                               int64_t removeCount, int lineNo);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, StorageMetricsTest);

INDEXLIB_UNIT_TEST_CASE(StorageMetricsTest, TestForFileMetrics);
INDEXLIB_UNIT_TEST_CASE(StorageMetricsTest, TestForFileCacheMetrics);

//////////////////////////////////////////////////////////////////////

StorageMetricsTest::StorageMetricsTest() {}

StorageMetricsTest::~StorageMetricsTest() {}

void StorageMetricsTest::CaseSetUp() {}

void StorageMetricsTest::CaseTearDown() {}

void StorageMetricsTest::CheckFileMetrics(StorageMetrics& metrics, FSFileType type)
{
    for (FSMetricGroup metricGroup : {FSMG_MEM, FSMG_LOCAL}) {
        metrics.IncreaseFile(metricGroup, type, 0);
        ASSERT_EQ(0, metrics.GetFileLength(metricGroup, type));
        ASSERT_EQ(1, metrics.GetFileCount(metricGroup, type));

        metrics.DecreaseFile(metricGroup, type, 0);
        ASSERT_EQ(0, metrics.GetFileLength(metricGroup, type));
        ASSERT_EQ(0, metrics.GetFileCount(metricGroup, type));

        metrics.IncreaseFile(metricGroup, type, 100);
        metrics.IncreaseFile(metricGroup, type, 200);
        metrics.DecreaseFile(metricGroup, type, 100);
        metrics.IncreaseFile(metricGroup, type, 300);
        metrics.DecreaseFile(metricGroup, type, 300);
        metrics.IncreaseFile(metricGroup, type, 300);

        ASSERT_EQ(500, metrics.GetFileLength(metricGroup, type));
        ASSERT_EQ(2, metrics.GetFileCount(metricGroup, type));
    }
}

void StorageMetricsTest::TestForFileMetrics()
{
    StorageMetrics metrics;
    CheckFileMetrics(metrics, FSFT_MEM);
    CheckFileMetrics(metrics, FSFT_MMAP);
    CheckFileMetrics(metrics, FSFT_BUFFERED);
    CheckFileMetrics(metrics, FSFT_BLOCK);
    CheckFileMetrics(metrics, FSFT_SLICE);

    ASSERT_EQ(20, metrics.GetTotalFileCount());
    ASSERT_EQ(5000, metrics.GetTotalFileLength());
}

void StorageMetricsTest::CheckFileCacheMetrics(const StorageMetrics& metrics, int64_t hitCount, int64_t missCount,
                                               int64_t replaceCount, int64_t removeCount, int lineNo)
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
}} // namespace indexlib::file_system
