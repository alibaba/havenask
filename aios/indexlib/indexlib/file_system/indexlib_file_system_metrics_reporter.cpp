#include "indexlib/file_system/indexlib_file_system_metrics_reporter.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/misc/indexlib_metric_control.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemMetricsReporter);

IndexlibFileSystemMetricsReporter::IndexlibFileSystemMetricsReporter(
        MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
{
}

IndexlibFileSystemMetricsReporter::~IndexlibFileSystemMetricsReporter() 
{
}

void IndexlibFileSystemMetricsReporter::DeclareMetrics(
        const IndexlibFileSystemMetrics& metrics,
        FSMetricPreference metricPref)
{
    DeclareStorageMetrics(metricPref);
}


void IndexlibFileSystemMetricsReporter::DeclareStorageMetrics(
        FSMetricPreference metricPref)
{
#define INIT_FILE_SYSTEM_METRIC(metric, unit)                                \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "file_system/"#metric, kmonitor::STATUS, unit)

    if (metricPref == FSMP_ONLINE)
    {
        INIT_FILE_SYSTEM_METRIC(MmapLockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(MmapNonLockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(BlockFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(SliceFileLength, "byte");
        INIT_FILE_SYSTEM_METRIC(BufferFileLength, "byte");
    }
            
    INIT_FILE_SYSTEM_METRIC(InMemFileLength, "byte");

    // INIT_FILE_SYSTEM_METRIC(InMemCacheHitCount, "count");
    // INIT_FILE_SYSTEM_METRIC(InMemCacheMissCount, "count");
    // INIT_FILE_SYSTEM_METRIC(InMemCacheReplaceCount, "count");
    // INIT_FILE_SYSTEM_METRIC(InMemCacheRemoveCount, "count");
    // INIT_FILE_SYSTEM_METRIC(InMemCacheFileCount, "count");

    // INIT_FILE_SYSTEM_METRIC(LocalCacheHitCount, "count");
    // INIT_FILE_SYSTEM_METRIC(LocalCacheMissCount, "count");
    // INIT_FILE_SYSTEM_METRIC(LocalCacheReplaceCount, "count");
    // INIT_FILE_SYSTEM_METRIC(LocalCacheRemoveCount, "count");
    // INIT_FILE_SYSTEM_METRIC(LocalCacheFileCount, "count");

    // INIT_FILE_SYSTEM_METRIC(MountPackageFileCount, "count");
    // INIT_FILE_SYSTEM_METRIC(MountPackageInnerFileCount, "count");
    // INIT_FILE_SYSTEM_METRIC(MountPackageInnerDirCount, "count");

    INIT_FILE_SYSTEM_METRIC(InMemStorageFlushMemoryUse, "byte");

    INIT_FILE_SYSTEM_METRIC(MemoryQuotaUse, "byte");

#undef INIT_FILE_SYSTEM_METRIC
}

void IndexlibFileSystemMetricsReporter::ReportMetrics(
        const IndexlibFileSystemMetrics& metrics)
{
    ReportStorageMetrics(metrics.GetInMemStorageMetrics(),
                         metrics.GetLocalStorageMetrics());   
}

void IndexlibFileSystemMetricsReporter::ReportMemoryQuotaUse(size_t memoryQuotaUse)
{
    IE_REPORT_METRIC(MemoryQuotaUse, memoryQuotaUse);
}

void IndexlibFileSystemMetricsReporter::ReportStorageMetrics(
        const StorageMetrics& inMemMetrics,
        const StorageMetrics& localMetrics)
{
    IE_REPORT_METRIC(InMemFileLength,
                     inMemMetrics.GetFileLength(FSFT_IN_MEM)
                     + localMetrics.GetFileLength(FSFT_IN_MEM));
    IE_REPORT_METRIC(MmapLockFileLength,
                     inMemMetrics.GetMmapLockFileLength()
                     + localMetrics.GetMmapLockFileLength());
    IE_REPORT_METRIC(MmapNonLockFileLength,
                     inMemMetrics.GetMmapNonlockFileLength()
                     + localMetrics.GetMmapNonlockFileLength());
    IE_REPORT_METRIC(BlockFileLength,
                     inMemMetrics.GetFileLength(FSFT_BLOCK)
                     + localMetrics.GetFileLength(FSFT_BLOCK));
    IE_REPORT_METRIC(BufferFileLength,
                     inMemMetrics.GetFileLength(FSFT_BUFFERED)
                     + localMetrics.GetFileLength(FSFT_BUFFERED));
    IE_REPORT_METRIC(SliceFileLength,
                     inMemMetrics.GetFileLength(FSFT_SLICE)
                     + localMetrics.GetFileLength(FSFT_SLICE));

    // IE_REPORT_METRIC(MountPackageFileCount, inMemMetrics.GetPackageFileCount() 
    //                  + localMetrics.GetPackageFileCount());
    // IE_REPORT_METRIC(MountPackageInnerFileCount, inMemMetrics.GetPackageInnerFileCount() 
    //                  + localMetrics.GetPackageInnerFileCount());
    // IE_REPORT_METRIC(MountPackageInnerDirCount, inMemMetrics.GetPackageInnerDirCount() 
    //                  + localMetrics.GetPackageInnerDirCount());
    
    IE_REPORT_METRIC(InMemStorageFlushMemoryUse, inMemMetrics.GetFlushMemoryUse());

    // const FileCacheMetrics& inMemCacheMetrics = inMemMetrics.GetFileCacheMetrics();
    // IE_REPORT_METRIC(InMemCacheHitCount, inMemCacheMetrics.hitCount.getValue());
    // IE_REPORT_METRIC(InMemCacheMissCount, inMemCacheMetrics.missCount.getValue());
    // IE_REPORT_METRIC(InMemCacheReplaceCount, inMemCacheMetrics.replaceCount.getValue());
    // IE_REPORT_METRIC(InMemCacheRemoveCount, inMemCacheMetrics.removeCount.getValue());
    IE_REPORT_METRIC(InMemCacheFileCount, inMemMetrics.GetTotalFileCount());

    // const FileCacheMetrics& localCacheMetrics = localMetrics.GetFileCacheMetrics();
    // IE_REPORT_METRIC(LocalCacheHitCount, localCacheMetrics.hitCount.getValue());
    // IE_REPORT_METRIC(LocalCacheMissCount, localCacheMetrics.missCount.getValue());
    // IE_REPORT_METRIC(LocalCacheReplaceCount, localCacheMetrics.replaceCount.getValue());
    // IE_REPORT_METRIC(LocalCacheRemoveCount, localCacheMetrics.removeCount.getValue());
    IE_REPORT_METRIC(LocalCacheFileCount, localMetrics.GetTotalFileCount());
}

IE_NAMESPACE_END(file_system);

