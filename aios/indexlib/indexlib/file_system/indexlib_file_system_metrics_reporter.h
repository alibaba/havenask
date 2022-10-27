#ifndef __INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_REPORTER_H
#define __INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_REPORTER_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/block_cache_metrics.h"
#include "indexlib/file_system/indexlib_file_system_metrics.h"
#include "indexlib/misc/monitor.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemMetricsReporter
{
public:
    IndexlibFileSystemMetricsReporter(
            misc::MetricProviderPtr metricProvider);
    ~IndexlibFileSystemMetricsReporter();

public:
    void DeclareMetrics(const IndexlibFileSystemMetrics& metrics,
                        FSMetricPreference metricPref);
    
    void ReportMetrics(const IndexlibFileSystemMetrics& metrics);
    void ReportMemoryQuotaUse(size_t memoryQuotaUse);

private:
    void DeclareStorageMetrics(FSMetricPreference metricPref);
    void ReportStorageMetrics(const StorageMetrics& inMemMetrics,
                              const StorageMetrics& localMetrics);

public:
    // for test
#define TEST_DEFINE_GET_METRIC(metricName)                           \
    const misc::MetricPtr& Get##metricName##Metric() const \
    { return m##metricName##Metric; }

    TEST_DEFINE_GET_METRIC(InMemFileLength);
    TEST_DEFINE_GET_METRIC(MmapLockFileLength);
    TEST_DEFINE_GET_METRIC(MmapNonLockFileLength);
    TEST_DEFINE_GET_METRIC(BlockFileLength);
    TEST_DEFINE_GET_METRIC(BufferFileLength);
    TEST_DEFINE_GET_METRIC(SliceFileLength);

#undef TEST_DEFINE_GET_METRIC
    
private:
    typedef std::vector<misc::MetricPtr> MetricSamplerVec;

private:
    misc::MetricProviderPtr mMetricProvider;

    IE_DECLARE_METRIC(InMemFileLength);
    IE_DECLARE_METRIC(MmapLockFileLength);
    IE_DECLARE_METRIC(MmapNonLockFileLength);
    IE_DECLARE_METRIC(BlockFileLength);
    IE_DECLARE_METRIC(BufferFileLength);
    IE_DECLARE_METRIC(SliceFileLength);

    // IE_DECLARE_METRIC(InMemCacheHitCount);
    // IE_DECLARE_METRIC(InMemCacheMissCount);
    // IE_DECLARE_METRIC(InMemCacheReplaceCount);
    // IE_DECLARE_METRIC(InMemCacheRemoveCount);
    IE_DECLARE_METRIC(InMemCacheFileCount);

    // IE_DECLARE_METRIC(LocalCacheHitCount);
    // IE_DECLARE_METRIC(LocalCacheMissCount);
    // IE_DECLARE_METRIC(LocalCacheReplaceCount);
    // IE_DECLARE_METRIC(LocalCacheRemoveCount);
    IE_DECLARE_METRIC(LocalCacheFileCount);

    // IE_DECLARE_METRIC(MountPackageFileCount);
    // IE_DECLARE_METRIC(MountPackageInnerFileCount);
    // IE_DECLARE_METRIC(MountPackageInnerDirCount);

    IE_DECLARE_METRIC(InMemStorageFlushMemoryUse);

    IE_DECLARE_METRIC(MemoryQuotaUse);

    MetricSamplerVec mCacheHitRatioVec;
    MetricSamplerVec mCacheLast1000AccessHitRatio;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibFileSystemMetricsReporter);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIB_FILE_SYSTEM_METRICS_REPORTER_H
