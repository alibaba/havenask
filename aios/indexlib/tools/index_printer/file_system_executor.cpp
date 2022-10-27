#include "tools/index_printer/file_system_executor.h"
#include "tools/index_printer/metrics_define.h"
#include "indexlib/file_system/indexlib_file_system_metrics.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/block_cache.h"

using namespace std;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(tools);
IE_LOG_SETUP(tools, FileSystemExecutor);

FileSystemExecutor::FileSystemExecutor() 
{
}

FileSystemExecutor::~FileSystemExecutor() 
{
}

void FileSystemExecutor::DoPrintStorageMetrics(const string& name,
        const StorageMetrics& metrics)
{
    const FileCacheMetrics& cacheMetrics = metrics.GetFileCacheMetrics();
    cout << setw(8) << name
         << setw(12) << metrics.GetTotalFileCount()
         << setw(12) << metrics.GetFileCount(FSFT_SLICE)
         << setw(12) << metrics.GetMmapLockFileLength()
         << setw(12) << metrics.GetMmapNonlockFileLength()
         << setw(16) << metrics.GetFileCount(FSFT_BLOCK)
         << setw(8) << cacheMetrics.missCount.getValue()
         << setw(8) << cacheMetrics.hitCount.getValue()
         << setw(8) << cacheMetrics.replaceCount.getValue()
         << setw(8) << cacheMetrics.removeCount.getValue()
         << endl;
}

bool FileSystemExecutor::PrintFileSystemMetrics(
        IndexPartition* partition, const string& line)
{
    IndexlibFileSystemMetrics metrics = partition->TEST_GetFileSystemMetrics();
    cout << "file system metrics:" << endl;
    cout << setiosflags(ios::left) 
         << setw(8) << "Storage"
         << setw(12) << "FileCount"
         << setw(12) << "SliceCount"
         << setw(12) << "MMapLock"
         << setw(12) << "MMapNonLock"
         << setw(16) << "CacheFileCount"
         << setw(8) << "Miss"
         << setw(8) << "Hit"
         << setw(8) << "Replace"
         << setw(8) << "Remove"
         << endl;
    DoPrintStorageMetrics("inmem", metrics.GetInMemStorageMetrics());
    DoPrintStorageMetrics("local", metrics.GetLocalStorageMetrics());

    return true;
}

bool FileSystemExecutor::PrintCacheHit(partition::IndexPartition* partition,
                                       const string& name)
{
    auto fileBlockCache = partition->TEST_GetFileBlockCache();

    if (fileBlockCache)
    {
        auto blockCache = fileBlockCache->GetBlockCache();
        auto hitCount = blockCache->GetTotalHitCount();
        auto missCount = blockCache->GetTotalMissCount();
        string metricsName = std::string("global.") + TOTAL_ACCESS_METRICS_SUFFIX;
        cout << metricsName << ":"
             << " " << hitCount + missCount << endl;

        metricsName = std::string("global.") + TOTAL_HITS_METRICS_SUFFIX;
        cout << metricsName << ":"
             << " " << hitCount << endl;

        //currently not support last 1000 hit count
         metricsName = name + "." + LAST1000_ACCESS_HITS_METRICS_SUFFIX;;
         cout << metricsName << ":" << " 0"  << endl;
        return true;
    }
    cout << "block cache not declared" << name << endl;
    return false;
}

IE_NAMESPACE_END(tools);

